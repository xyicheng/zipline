"""
Tests for chunked adjustments.
"""
from itertools import chain
from textwrap import dedent
from unittest import TestCase

from nose_parameterized import parameterized
from numpy import (
    arange,
    array,
    asarray,
    dtype,
    full,
    where,
)
from six.moves import zip_longest
from toolz import curry

from zipline.errors import WindowLengthNotPositive, WindowLengthTooLong
from zipline.lib.adjustment import (
    Datetime64Overwrite,
    Float64Multiply,
    Float64Overwrite,
    ObjectOverwrite,
)
from zipline.lib.adjusted_array import AdjustedArray, NOMASK
from zipline.lib.labelarray import LabelArray
from zipline.testing import check_arrays, parameter_space
from zipline.utils.numpy_utils import (
    coerce_to_dtype,
    datetime64ns_dtype,
    default_missing_value_for_dtype,
    float64_dtype,
    int64_dtype,
    object_dtype,
)


def moving_window(array, nrows):
    """
    Simple moving window generator over a 2D numpy array.
    """
    count = num_windows_of_length_M_on_buffers_of_length_N(nrows, len(array))
    for i in range(count):
        yield array[i:i + nrows]


def num_windows_of_length_M_on_buffers_of_length_N(M, N):
    """
    For a window of length M rolling over a buffer of length N,
    there are (N - M) + 1 legal windows.

    Example:
    If my array has N=4 rows, and I want windows of length M=2, there are
    3 legal windows: data[0:2], data[1:3], and data[2:4].
    """
    return N - M + 1


def valid_window_lengths(underlying_buffer_length):
    """
    An iterator of all legal window lengths on a buffer of a given length.

    Returns values from 1 to underlying_buffer_length.
    """
    return iter(range(1, underlying_buffer_length + 1))


@curry
def as_dtype(dtype, data):
    """
    Curried wrapper around array.astype for when you have the dtype before you
    have the data.
    """
    return asarray(data).astype(dtype)


@curry
def as_labelarray(initial_dtype, missing_value, array):
    """
    Curried wrapper around LabelArray, that round-trips the input data through
    `initial_dtype` first.
    """
    return LabelArray(
        array.astype(initial_dtype),
        missing_value=initial_dtype.type(''),
    )


bytes_dtype = dtype('S3')
unicode_dtype = dtype('U3')


def _gen_unadjusted_cases(name,
                          make_input,
                          make_expected_output,
                          missing_value):
    nrows = 6
    ncols = 3

    raw_data = arange(nrows * ncols).reshape(nrows, ncols)
    input_array = make_input(raw_data)
    expected_output_array = make_expected_output(raw_data)

    for windowlen in valid_window_lengths(nrows):

        num_legal_windows = num_windows_of_length_M_on_buffers_of_length_N(
            windowlen, nrows
        )

        yield (
            "%s_length_%d" % (name, windowlen),
            input_array,
            windowlen,
            {},
            missing_value,
            [
                expected_output_array[offset:offset + windowlen]
                for offset in range(num_legal_windows)
            ],
        )


def _gen_multiplicative_adjustment_cases(dtype):
    """
    Generate expected moving windows on a buffer with adjustments.

    We proceed by constructing, at each row, the view of the array we expect in
    in all windows anchored on or after that row.

    In general, if we have an adjustment to be applied once we process the row
    at index N, should see that adjustment applied to the underlying buffer for
    any window containing the row at index N.

    We then build all legal windows over these buffers.
    """
    adjustment_type = {
        float64_dtype: Float64Multiply,
    }[dtype]

    nrows, ncols = 6, 3
    adjustments = {}
    buffer_as_of = [None] * 6
    baseline = buffer_as_of[0] = full((nrows, ncols), 1, dtype=dtype)

    # Note that row indices are inclusive!
    adjustments[1] = [
        adjustment_type(0, 0, 0, 0, coerce_to_dtype(dtype, 2)),
    ]
    buffer_as_of[1] = array([[2, 1, 1],
                             [1, 1, 1],
                             [1, 1, 1],
                             [1, 1, 1],
                             [1, 1, 1],
                             [1, 1, 1]], dtype=dtype)

    # No adjustment at index 2.
    buffer_as_of[2] = buffer_as_of[1]

    adjustments[3] = [
        adjustment_type(1, 2, 1, 1, coerce_to_dtype(dtype, 3)),
        adjustment_type(0, 1, 0, 0, coerce_to_dtype(dtype, 4)),
    ]
    buffer_as_of[3] = array([[8, 1, 1],
                             [4, 3, 1],
                             [1, 3, 1],
                             [1, 1, 1],
                             [1, 1, 1],
                             [1, 1, 1]], dtype=dtype)

    adjustments[4] = [
        adjustment_type(0, 3, 2, 2, coerce_to_dtype(dtype, 5))
    ]
    buffer_as_of[4] = array([[8, 1, 5],
                             [4, 3, 5],
                             [1, 3, 5],
                             [1, 1, 5],
                             [1, 1, 1],
                             [1, 1, 1]], dtype=dtype)

    adjustments[5] = [
        adjustment_type(0, 4, 1, 1, coerce_to_dtype(dtype, 6)),
        adjustment_type(2, 2, 2, 2, coerce_to_dtype(dtype, 7)),
    ]
    buffer_as_of[5] = array([[8,  6,  5],
                             [4, 18,  5],
                             [1, 18, 35],
                             [1,  6,  5],
                             [1,  6,  1],
                             [1,  1,  1]], dtype=dtype)

    return _gen_expectations(
        baseline,
        default_missing_value_for_dtype(dtype),
        adjustments,
        buffer_as_of,
        nrows,
    )


def _gen_overwrite_adjustment_cases(make_input, make_expected_output, dtype):
    """
    Generate test cases for overwrite adjustments.

    The algorithm used here is the same as the one used above for
    multiplicative adjustments.  The only difference is the semantics of how
    the adjustments are expected to modify the arrays.

    This is parameterized on `make_input` and make_expected_output functions,
    which take 2-D lists of values and transform them into desired input/output
    arrays. We do this so that we can easily test both vanilla numpy ndarrays
    and our own LabelArray class for strings.
    """
    adjustment_type = {
        float64_dtype: Float64Overwrite,
        datetime64ns_dtype: Datetime64Overwrite,
        bytes_dtype: ObjectOverwrite,
        unicode_dtype: ObjectOverwrite,
        object_dtype: ObjectOverwrite,
    }[dtype]

    if dtype == object_dtype:
        # When we're testing object dtypes, we expect to have strings, but
        # coerce_to_dtype(object, 3) just gives 3 as a Python integer.
        def make_overwrite_value(dtype, value):
            return str(value)
    else:
        make_overwrite_value = coerce_to_dtype

    adjustments = {}
    buffer_as_of = [None] * 6
    baseline = make_input([[2, 2, 2],
                           [2, 2, 2],
                           [2, 2, 2],
                           [2, 2, 2],
                           [2, 2, 2],
                           [2, 2, 2]])

    buffer_as_of[0] = make_expected_output([[2, 2, 2],
                                            [2, 2, 2],
                                            [2, 2, 2],
                                            [2, 2, 2],
                                            [2, 2, 2],
                                            [2, 2, 2]])

    # Note that row indices are inclusive!
    adjustments[1] = [
        adjustment_type(0, 0, 0, 0, make_overwrite_value(dtype, 1)),
    ]
    buffer_as_of[1] = make_expected_output([[1, 2, 2],
                                            [2, 2, 2],
                                            [2, 2, 2],
                                            [2, 2, 2],
                                            [2, 2, 2],
                                            [2, 2, 2]])

    # No adjustment at index 2.
    buffer_as_of[2] = buffer_as_of[1]

    adjustments[3] = [
        adjustment_type(1, 2, 1, 1, make_overwrite_value(dtype, 3)),
        adjustment_type(0, 1, 0, 0, make_overwrite_value(dtype, 4)),
    ]
    buffer_as_of[3] = make_expected_output([[4, 2, 2],
                                            [4, 3, 2],
                                            [2, 3, 2],
                                            [2, 2, 2],
                                            [2, 2, 2],
                                            [2, 2, 2]])

    adjustments[4] = [
        adjustment_type(0, 3, 2, 2, make_overwrite_value(dtype, 5))
    ]
    buffer_as_of[4] = make_expected_output([[4, 2, 5],
                                            [4, 3, 5],
                                            [2, 3, 5],
                                            [2, 2, 5],
                                            [2, 2, 2],
                                            [2, 2, 2]])

    adjustments[5] = [
        adjustment_type(0, 4, 1, 1, make_overwrite_value(dtype, 6)),
        adjustment_type(2, 2, 2, 2, make_overwrite_value(dtype, 7)),
    ]
    buffer_as_of[5] = make_expected_output([[4, 6, 5],
                                            [4, 6, 5],
                                            [2, 6, 7],
                                            [2, 6, 5],
                                            [2, 6, 2],
                                            [2, 2, 2]])

    return _gen_expectations(
        baseline,
        default_missing_value_for_dtype(dtype),
        adjustments,
        buffer_as_of,
        nrows,
    )


def _gen_expectations(baseline, missing_value, adjustments, buffer_as_of, nrows):

    for windowlen in valid_window_lengths(nrows):

        num_legal_windows = num_windows_of_length_M_on_buffers_of_length_N(
            windowlen, nrows
        )

        yield (
            "dtype_%s_length_%d" % (baseline.dtype, windowlen),
            baseline,
            windowlen,
            adjustments,
            missing_value,
            [
                # This is a nasty expression...
                #
                # Reading from right to left: we want a slice of length
                # 'windowlen', starting at 'offset', from the buffer on which
                # we've applied all adjustments corresponding to the last row
                # of the data, which will be (offset + windowlen - 1).
                buffer_as_of[offset + windowlen - 1][offset:offset + windowlen]
                for offset in range(num_legal_windows)
            ],
        )


class AdjustedArrayTestCase(TestCase):

    @parameterized.expand(
        chain(
            _gen_unadjusted_cases(
                'float',
                make_input=as_dtype(float64_dtype),
                make_expected_output=as_dtype(float64_dtype),
                missing_value=default_missing_value_for_dtype(float64_dtype),
            ),
            _gen_unadjusted_cases(
                'datetime',
                make_input=as_dtype(datetime64ns_dtype),
                make_expected_output=as_dtype(datetime64ns_dtype),
                missing_value=default_missing_value_for_dtype(
                    datetime64ns_dtype
                ),
            ),
            # Test passing an array of strings to AdjustedArray.
            _gen_unadjusted_cases(
                'bytes_ndarray',
                make_input=as_dtype(bytes_dtype),
                make_expected_output=as_labelarray(bytes_dtype, b''),
                missing_value=b'',
            ),
            _gen_unadjusted_cases(
                'unicode_ndarray',
                make_input=as_dtype(unicode_dtype),
                make_expected_output=as_labelarray(unicode_dtype, u''),
                missing_value=u'',
            ),
            _gen_unadjusted_cases(
                'object_ndarray',
                make_input=lambda a: a.astype(str).astype(object),
                make_expected_output=as_labelarray(bytes_dtype, b''),
                missing_value=b'',
            ),
            # Test passing a LabelArray directly to AdjustedArray.
            _gen_unadjusted_cases(
                'bytes_labelarray',
                make_input=as_labelarray(bytes_dtype, b''),
                make_expected_output=as_labelarray(bytes_dtype, b''),
                missing_value=b'',
            ),
            _gen_unadjusted_cases(
                'unicode_labelarray',
                make_input=as_labelarray(unicode_dtype, u''),
                make_expected_output=as_labelarray(bytes_dtype, u''),
                missing_value=u'',
            ),
            _gen_unadjusted_cases(
                'object_labelarray',
                make_input=(
                    lambda a: LabelArray(a.astype(str).astype(object), b'')
                ),
                make_expected_output=as_labelarray(bytes_dtype, b''),
                missing_value=b'',
            ),
        )
    )
    def test_no_adjustments(self,
                            name,
                            data,
                            lookback,
                            adjustments,
                            missing_value,
                            expected_output):

        array = AdjustedArray(data, NOMASK, adjustments, missing_value)
        for _ in range(2):  # Iterate 2x ensure adjusted_arrays are re-usable.
            in_out = zip(array.traverse(lookback), expected_output)
            for yielded, expected_yield in in_out:
                check_arrays(yielded, expected_yield)

    @parameterized.expand(_gen_multiplicative_adjustment_cases(float64_dtype))
    def test_multiplicative_adjustments(self,
                                        name,
                                        data,
                                        lookback,
                                        adjustments,
                                        missing_value,
                                        expected):

        array = AdjustedArray(data, NOMASK, adjustments, missing_value)
        for _ in range(2):  # Iterate 2x ensure adjusted_arrays are re-usable.
            window_iter = array.traverse(lookback)
            for yielded, expected_yield in zip_longest(window_iter, expected):
                check_arrays(yielded, expected_yield)

    @parameterized.expand(
        chain(
            _gen_overwrite_adjustment_cases(
                make_input=as_dtype(float64_dtype),
                make_expected_output=as_dtype(float64_dtype),
                dtype=float64_dtype,
            ),
            _gen_overwrite_adjustment_cases(
                make_input=as_dtype(datetime64ns_dtype),
                make_expected_output=as_dtype(datetime64ns_dtype),
                dtype=datetime64ns_dtype,
            ),
        )
    )
    def test_overwrite_adjustment_cases(self,
                                        name,
                                        data,
                                        lookback,
                                        adjustments,
                                        missing_value,
                                        expected):
        array = AdjustedArray(data, NOMASK, adjustments, missing_value)
        for _ in range(2):  # Iterate 2x ensure adjusted_arrays are re-usable.
            window_iter = array.traverse(lookback)
            for yielded, expected_yield in zip_longest(window_iter, expected):
                self.assertEqual(yielded.dtype, data.dtype)
                check_arrays(yielded, expected_yield)

    @parameter_space(
        __fail_fast=True,
        dtype=[
            float64_dtype,
            int64_dtype,
            datetime64ns_dtype,
        ],
        missing_value=[0, 10000],
        window_length=[2, 3],
    )
    def test_masking(self, dtype, missing_value, window_length):
        missing_value = coerce_to_dtype(dtype, missing_value)
        baseline_ints = arange(15).reshape(5, 3)
        baseline = baseline_ints.astype(dtype)
        mask = (baseline_ints % 2).astype(bool)
        masked_baseline = where(mask, baseline, missing_value)

        array = AdjustedArray(
            baseline,
            mask,
            adjustments={},
            missing_value=missing_value,
        )

        gen_expected = moving_window(masked_baseline, window_length)
        gen_actual = array.traverse(window_length)
        for expected, actual in zip(gen_expected, gen_actual):
            check_arrays(expected, actual)

    @parameter_space(
        __fail_fast=True,
        dtype=[bytes_dtype, unicode_dtype, object_dtype],
        missing_value=["0", "-1", ""],
        window_length=[2, 3],
    )
    def test_masking_with_strings(self, dtype, missing_value, window_length):
        missing_value = coerce_to_dtype(dtype, missing_value)
        baseline_ints = arange(15).reshape(5, 3)

        # Coerce to string first so that coercion to object gets us an array of
        # string objects.
        baseline = baseline_ints.astype(str).astype(dtype)
        mask = (baseline_ints % 2).astype(bool)

        masked_baseline = LabelArray(baseline, missing_value=missing_value)
        masked_baseline[~mask] = missing_value

        array = AdjustedArray(
            baseline,
            mask,
            adjustments={},
            missing_value=missing_value,
        )

        gen_expected = moving_window(masked_baseline, window_length)
        gen_actual = array.traverse(window_length=window_length)

        for expected, actual in zip(gen_expected, gen_actual):
            check_arrays(expected, actual)

    def test_invalid_lookback(self):

        data = arange(30, dtype=float).reshape(6, 5)
        adj_array = AdjustedArray(data, NOMASK, {}, float('nan'))

        with self.assertRaises(WindowLengthTooLong):
            adj_array.traverse(7)

        with self.assertRaises(WindowLengthNotPositive):
            adj_array.traverse(0)

        with self.assertRaises(WindowLengthNotPositive):
            adj_array.traverse(-1)

    def test_array_views_arent_writable(self):

        data = arange(30, dtype=float).reshape(6, 5)
        adj_array = AdjustedArray(data, NOMASK, {}, float('nan'))

        for frame in adj_array.traverse(3):
            with self.assertRaises(ValueError):
                frame[0, 0] = 5.0

    def test_bad_input(self):
        msg = "Mask shape \(2L?, 3L?\) != data shape \(5L?, 5L?\)"
        data = arange(25).reshape(5, 5)
        bad_mask = array([[0, 1, 1], [0, 0, 1]], dtype=bool)

        with self.assertRaisesRegexp(ValueError, msg):
            AdjustedArray(data, bad_mask, {}, missing_value=-1)

    def test_inspect(self):
        data = arange(15, dtype=float).reshape(5, 3)
        adj_array = AdjustedArray(
            data,
            NOMASK,
            {4: [Float64Multiply(2, 3, 0, 0, 4.0)]},
            float('nan'),
        )

        expected = dedent(
            """\
            Adjusted Array (float64):

            Data:
            array([[  0.,   1.,   2.],
                   [  3.,   4.,   5.],
                   [  6.,   7.,   8.],
                   [  9.,  10.,  11.],
                   [ 12.,  13.,  14.]])

            Adjustments:
            {4: [Float64Multiply(first_row=2, last_row=3, first_col=0, \
last_col=0, value=4.000000)]}
            """
        )
        got = adj_array.inspect()
        self.assertEqual(expected, got)
