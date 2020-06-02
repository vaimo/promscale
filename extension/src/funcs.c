/* This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE for a copy of the license
 */


#include <postgres.h>
#include <catalog/pg_type_d.h>
#include <datatype/timestamp.h>
#include <fmgr.h>

#include <utils/array.h>
#include <utils/palloc.h>

#include <nodes/pathnodes.h>
#include <nodes/supportnodes.h>
#include <nodes/bitmapset.h>
#include <optimizer/optimizer.h>
#include <catalog/pg_type.h>
#include <utils/timestamp.h>
#include <utils/datetime.h>


PG_FUNCTION_INFO_V1(gapfill_array_delta);


/* Taken from check_float8_array in PG src, with slight modifications */
static float8 *
check_float8_array(ArrayType *transarray, const char *caller)
{
	/*
	 * We expect the input to be an N-element float array; verify that. We
	 * don't need to use deconstruct_array() since the array data is just
	 * going to look like a C array of N float8 values.
	 */
	if (ARR_NDIM(transarray) != 1 ||
		ARR_HASNULL(transarray) ||
		ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "%s: expected float8 array", caller);
	return (float8 *) ARR_DATA_PTR(transarray);
}

static Datum *
check_timestamptz_array(ArrayType *array, const char *caller)
{
    Datum *datums;
    int ndatums;

	if (ARR_NDIM(array) != 1 ||
		ARR_HASNULL(array) ||
		ARR_ELEMTYPE(array) != TIMESTAMPTZOID)
		elog(ERROR, "%s: expected timestamptz array", caller);

    /* Todo check if we can avoid deconstruct object by playing same trick as float8 above? */
	deconstruct_array(array,
					  TIMESTAMPTZOID, sizeof(TimestampTz), true, 'd',
					  &datums, NULL, &ndatums);
    return datums;
}

Datum
gapfill_array_delta(PG_FUNCTION_ARGS)
{
   TimestampTz start_ts = PG_GETARG_TIMESTAMPTZ(0);
   TimestampTz end_ts = PG_GETARG_TIMESTAMPTZ(1);
   int64 step_s = PG_GETARG_INT64(2);
   int64 range_s = PG_GETARG_INT64(3);
   ArrayType  *t_array = PG_GETARG_ARRAYTYPE_P(4);
   ArrayType  *v_array = PG_GETARG_ARRAYTYPE_P(5);
   float8 *v_elem;
   Datum *t_datums;
   int t_length = ARR_DIMS(t_array)[0];
   int v_length = ARR_DIMS(v_array)[0];

   TimestampTz current_bucket_end_ts = end_ts;
   TimestampTz range_start_ts;
   int current_bucket_end_idx = 0;
   int range_start_idx = 0;
   int num_elements = ((end_ts - start_ts) / (step_s * USECS_PER_SEC))+1;
   Datum *res = palloc0(sizeof(Datum) * num_elements);
   ArrayType *res_array;
  int			res_dims[1];
	int			res_lbs[1];
   bool *res_null = palloc0(sizeof(bool) * num_elements);
   int res_idx = 0;

   Assert(ARR_ELEMTYPE(t_array) == TIMESTAMPTZOID);
   Assert(ARR_ELEMTYPE(v_array) == FLOAT8OID);
   Assert(t_length == v_length);

   v_elem = check_float8_array(v_array,"gapfill array delta");
   t_datums = check_timestamptz_array(t_array,"gapfill array delta");

    while(current_bucket_end_ts > start_ts)
    {
        while(current_bucket_end_idx < t_length && DatumGetTimestampTz(t_datums[current_bucket_end_idx]) > current_bucket_end_ts)
        {
            current_bucket_end_idx++;
        }

        range_start_ts = current_bucket_end_ts - (range_s * USECS_PER_SEC);
        if (current_bucket_end_idx > range_start_idx)
        {
            range_start_idx = current_bucket_end_idx;
        }
        while(range_start_idx < t_length && DatumGetTimestampTz(t_datums[range_start_idx]) > range_start_ts)
        {
            range_start_idx++;
        }


        /* need two elements */
        if (DatumGetTimestampTz(t_datums[range_start_idx-1]) > range_start_ts && range_start_idx - current_bucket_end_idx >= 2)
        {
            float8 last = v_elem[current_bucket_end_idx];
            float8 first = v_elem[range_start_idx-1];
            res[res_idx++] = Float8GetDatum(last - first);
        }
        else
        {
            res_null[res_idx++] = true;
        }

        current_bucket_end_ts = current_bucket_end_ts - (step_s * USECS_PER_SEC);
    }


    res_dims[0] = res_idx;
	  res_lbs[0] = 1;


    res_array = construct_md_array(res, res_null, 1, res_dims, res_lbs, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    PG_RETURN_ARRAYTYPE_P(res_array);
}
