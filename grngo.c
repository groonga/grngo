#include "grngo.h"

#include <string.h>

#define GRNGO_MAX_DATA_TYPE_ID GRN_DB_WGS84_GEO_POINT

grn_rc grngo_find_table(grn_ctx *ctx, const char *name, size_t name_len,
                        grn_obj **table) {
  if (!ctx || !name || !table) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj *obj = grn_ctx_get(ctx, name, name_len);
  if (!obj) {
    if (ctx->rc != GRN_SUCCESS) {
      return ctx->rc;
    }
    return GRN_UNKNOWN_ERROR;
  }
  switch (obj->header.type) {
    case GRN_TABLE_HASH_KEY:
    case GRN_TABLE_PAT_KEY:
    case GRN_TABLE_DAT_KEY:
    case GRN_TABLE_NO_KEY: {
      *table = obj;
      return GRN_SUCCESS;
    }
    default: {
      // The object is not a table.
      grn_obj_unlink(ctx, obj);
      return GRN_INVALID_FORMAT;
    }
  }
}

// grngo_init_type_info() initializes the members of type_info.
// The initialized type info specifies a valid Void type.
static void grngo_init_type_info(grngo_type_info *type_info) {
  type_info->data_type = GRN_DB_VOID;
  type_info->dimension = 0;
  type_info->ref_table = NULL;
}

grn_bool grngo_table_get_key_info(grn_ctx *ctx, grn_obj *table,
                                  grngo_type_info *key_info) {
  grngo_init_type_info(key_info);
  while (table) {
    switch (table->header.type) {
      case GRN_TABLE_HASH_KEY:
      case GRN_TABLE_PAT_KEY:
      case GRN_TABLE_DAT_KEY: {
        if (table->header.domain <= GRNGO_MAX_DATA_TYPE_ID) {
          key_info->data_type = table->header.domain;
          return GRN_TRUE;
        }
        table = grn_ctx_at(ctx, table->header.domain);
        if (!table) {
          return GRN_FALSE;
        }
        if (!key_info->ref_table) {
          key_info->ref_table = table;
        }
        break;
      }
      case GRN_TABLE_NO_KEY: {
        // GRN_DB_VOID, if the table has no key.
        return GRN_TRUE;
      }
      default: {
        // The object is not a table.
        return GRN_FALSE;
      }
    }
  }
  return GRN_FALSE;
}

grn_bool grngo_table_get_value_info(grn_ctx *ctx, grn_obj *table,
                                    grngo_type_info *value_info) {
  grngo_init_type_info(value_info);
  if (!table) {
    return GRN_FALSE;
  }
  switch (table->header.type) {
    case GRN_TABLE_HASH_KEY:
    case GRN_TABLE_PAT_KEY:
    case GRN_TABLE_DAT_KEY:
    case GRN_TABLE_NO_KEY: {
      grn_id range = grn_obj_get_range(ctx, table);
      if (range <= GRNGO_MAX_DATA_TYPE_ID) {
        value_info->data_type = range;
        return GRN_TRUE;
      }
      value_info->ref_table = grn_ctx_at(ctx, range);
      grngo_type_info key_info;
      if (!grngo_table_get_key_info(ctx, value_info->ref_table, &key_info)) {
        return GRN_FALSE;
      }
      value_info->data_type = key_info.data_type;
      return GRN_TRUE;
    }
    default: {
      // The object is not a table.
      return GRN_FALSE;
    }
  }
}

grn_bool grngo_column_get_value_info(grn_ctx *ctx, grn_obj *column,
                                     grngo_type_info *value_info) {
  grngo_init_type_info(value_info);
  if (!column) {
    return GRN_FALSE;
  }
  switch (column->header.type) {
    case GRN_COLUMN_FIX_SIZE: {
      break;
    }
    case GRN_COLUMN_VAR_SIZE: {
      grn_obj_flags type = column->header.flags & GRN_OBJ_COLUMN_TYPE_MASK;
      if (type == GRN_OBJ_COLUMN_VECTOR) {
        ++value_info->dimension;
      }
      break;
    }
    default: {
      // The object is not a data column.
      return GRN_FALSE;
    }
  }
  grn_id range = grn_obj_get_range(ctx, column);
  if (range <= GRNGO_MAX_DATA_TYPE_ID) {
    value_info->data_type = range;
    return GRN_TRUE;
  }
  value_info->ref_table = grn_ctx_at(ctx, range);
  grngo_type_info key_info;
  if (!grngo_table_get_key_info(ctx, value_info->ref_table, &key_info)) {
    return GRN_FALSE;
  }
  value_info->data_type = key_info.data_type;
  return GRN_TRUE;
}

char *grngo_table_get_name(grn_ctx *ctx, grn_obj *table) {
  if (!table) {
    return NULL;
  }
  switch (table->header.type) {
    case GRN_TABLE_HASH_KEY:
    case GRN_TABLE_PAT_KEY:
    case GRN_TABLE_DAT_KEY:
    case GRN_TABLE_NO_KEY: {
      break;
    }
    default: {
      // The object is not a table.
      return NULL;
    }
  }
  char buf[GRN_TABLE_MAX_KEY_SIZE];
  int len = grn_obj_name(ctx, table, buf, GRN_TABLE_MAX_KEY_SIZE);
  if (len <= 0) {
    return NULL;
  }
  char *table_name = (char *)malloc(len + 1);
  if (!table_name) {
    return NULL;
  }
  memcpy(table_name, buf, len);
  table_name[len] = '\0';
  return table_name;
}

// grngo_table_insert_row() calls grn_table_add() and converts the result.
static grngo_row_info grngo_table_insert_row(
    grn_ctx *ctx, grn_obj *table, const void *key_ptr, size_t key_size) {
  grngo_row_info row_info;
  int inserted;
  row_info.id = grn_table_add(ctx, table, key_ptr, key_size, &inserted);
  row_info.inserted = inserted ? GRN_TRUE : GRN_FALSE;
  return row_info;
}

grngo_row_info grngo_table_insert_void(grn_ctx *ctx, grn_obj *table) {
  return grngo_table_insert_row(ctx, table, NULL, 0);
}

grngo_row_info grngo_table_insert_bool(grn_ctx *ctx, grn_obj *table,
                                       grn_bool key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grngo_row_info grngo_table_insert_int8(grn_ctx *ctx, grn_obj *table,
                                       int8_t key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grngo_row_info grngo_table_insert_int16(grn_ctx *ctx, grn_obj *table,
                                        int16_t key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grngo_row_info grngo_table_insert_int32(grn_ctx *ctx, grn_obj *table,
                                        int32_t key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grngo_row_info grngo_table_insert_int64(grn_ctx *ctx, grn_obj *table,
                                        int64_t key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grngo_row_info grngo_table_insert_uint8(grn_ctx *ctx, grn_obj *table,
                                        uint8_t key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grngo_row_info grngo_table_insert_uint16(grn_ctx *ctx, grn_obj *table,
                                         uint16_t key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grngo_row_info grngo_table_insert_uint32(grn_ctx *ctx, grn_obj *table,
                                         uint32_t key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grngo_row_info grngo_table_insert_uint64(grn_ctx *ctx, grn_obj *table,
                                         uint64_t key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grngo_row_info grngo_table_insert_time(grn_ctx *ctx, grn_obj *table,
                                       int64_t key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grngo_row_info grngo_table_insert_float(grn_ctx *ctx, grn_obj *table,
                                        double key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grngo_row_info grngo_table_insert_text(grn_ctx *ctx, grn_obj *table,
                                       const grngo_text *key) {
  return grngo_table_insert_row(ctx, table, key->ptr, key->size);
}

grngo_row_info grngo_table_insert_geo_point(grn_ctx *ctx, grn_obj *table,
                                            grn_geo_point key) {
  return grngo_table_insert_row(ctx, table, &key, sizeof(key));
}

grn_bool grngo_column_set_bool(grn_ctx *ctx, grn_obj *column,
                               grn_id id, grn_bool value) {
  grn_obj obj;
  GRN_BOOL_INIT(&obj, 0);
  GRN_BOOL_SET(ctx, &obj, value);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_int8(grn_ctx *ctx, grn_obj *column,
                               grn_id id, int8_t value) {
  grn_obj obj;
  GRN_INT8_INIT(&obj, 0);
  GRN_INT8_SET(ctx, &obj, value);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_int16(grn_ctx *ctx, grn_obj *column,
                                grn_id id, int16_t value) {
  grn_obj obj;
  GRN_INT16_INIT(&obj, 0);
  GRN_INT16_SET(ctx, &obj, value);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_int32(grn_ctx *ctx, grn_obj *column,
                                grn_id id, int32_t value) {
  grn_obj obj;
  GRN_INT32_INIT(&obj, 0);
  GRN_INT32_SET(ctx, &obj, value);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_int64(grn_ctx *ctx, grn_obj *column,
                                grn_id id, int64_t value) {
  grn_obj obj;
  GRN_INT64_INIT(&obj, 0);
  GRN_INT64_SET(ctx, &obj, value);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_uint8(grn_ctx *ctx, grn_obj *column,
                                grn_id id, uint8_t value) {
  grn_obj obj;
  GRN_UINT8_INIT(&obj, 0);
  GRN_UINT8_SET(ctx, &obj, value);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_uint16(grn_ctx *ctx, grn_obj *column,
                                 grn_id id, uint16_t value) {
  grn_obj obj;
  GRN_UINT16_INIT(&obj, 0);
  GRN_UINT16_SET(ctx, &obj, value);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_uint32(grn_ctx *ctx, grn_obj *column,
                                 grn_id id, uint32_t value) {
  grn_obj obj;
  GRN_UINT32_INIT(&obj, 0);
  GRN_UINT32_SET(ctx, &obj, value);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_uint64(grn_ctx *ctx, grn_obj *column,
                                 grn_id id, uint64_t value) {
  grn_obj obj;
  GRN_UINT64_INIT(&obj, 0);
  GRN_UINT64_SET(ctx, &obj, value);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_time(grn_ctx *ctx, grn_obj *column,
                               grn_id id, int64_t value) {
  grn_obj obj;
  GRN_TIME_INIT(&obj, 0);
  GRN_TIME_SET(ctx, &obj, value);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_float(grn_ctx *ctx, grn_obj *column,
                                grn_id id, double value) {
  grn_obj obj;
  GRN_FLOAT_INIT(&obj, 0);
  GRN_FLOAT_SET(ctx, &obj, value);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_text(grn_ctx *ctx, grn_obj *column,
                               grn_id id, const grngo_text *value) {
  grn_obj obj;
  GRN_TEXT_INIT(&obj, 0);
  if (value) {
    GRN_TEXT_SET(ctx, &obj, value->ptr, value->size);
  } else {
    GRN_TEXT_SET(ctx, &obj, NULL, 0);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_geo_point(grn_ctx *ctx, grn_obj *column,
                                    grn_builtin_type data_type,
                                    grn_id id, grn_geo_point value) {
  grn_obj obj;
  if (data_type == GRN_DB_TOKYO_GEO_POINT) {
    GRN_TOKYO_GEO_POINT_INIT(&obj, 0);
  } else {
    GRN_WGS84_GEO_POINT_INIT(&obj, 0);
  }
  GRN_GEO_POINT_SET(ctx, &obj, value.latitude, value.longitude);
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_bool_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id,
                                      const grngo_vector *value) {
  grn_obj obj;
  GRN_BOOL_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  for (i = 0; i < value->size; i++) {
    GRN_BOOL_SET_AT(ctx, &obj, i, ((const grn_bool *)value->ptr)[i]);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_int8_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id,
                                      const grngo_vector *value) {
  grn_obj obj;
  GRN_INT8_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  for (i = 0; i < value->size; i++) {
    GRN_INT8_SET_AT(ctx, &obj, i, ((const int64_t *)value->ptr)[i]);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_int16_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id,
                                       const grngo_vector *value) {
  grn_obj obj;
  GRN_INT16_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  for (i = 0; i < value->size; i++) {
    GRN_INT16_SET_AT(ctx, &obj, i, ((const int64_t *)value->ptr)[i]);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_int32_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id,
                                       const grngo_vector *value) {
  grn_obj obj;
  GRN_INT32_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  for (i = 0; i < value->size; i++) {
    GRN_INT32_SET_AT(ctx, &obj, i, ((const int64_t *)value->ptr)[i]);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_int64_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id,
                                       const grngo_vector *value) {
  grn_obj obj;
  GRN_INT64_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  for (i = 0; i < value->size; i++) {
    GRN_INT64_SET_AT(ctx, &obj, i, ((const int64_t *)value->ptr)[i]);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_uint8_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id,
                                       const grngo_vector *value) {
  grn_obj obj;
  GRN_UINT8_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  for (i = 0; i < value->size; i++) {
    GRN_UINT8_SET_AT(ctx, &obj, i, ((const int64_t *)value->ptr)[i]);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_uint16_vector(grn_ctx *ctx, grn_obj *column,
                                        grn_id id,
                                        const grngo_vector *value) {
  grn_obj obj;
  GRN_UINT16_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  for (i = 0; i < value->size; i++) {
    GRN_UINT16_SET_AT(ctx, &obj, i, ((const int64_t *)value->ptr)[i]);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_uint32_vector(grn_ctx *ctx, grn_obj *column,
                                        grn_id id,
                                        const grngo_vector *value) {
  grn_obj obj;
  GRN_UINT32_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  for (i = 0; i < value->size; i++) {
    GRN_UINT32_SET_AT(ctx, &obj, i, ((const int64_t *)value->ptr)[i]);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_uint64_vector(grn_ctx *ctx, grn_obj *column,
                                        grn_id id,
                                        const grngo_vector *value) {
  grn_obj obj;
  GRN_UINT64_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  for (i = 0; i < value->size; i++) {
    GRN_UINT64_SET_AT(ctx, &obj, i, ((const int64_t *)value->ptr)[i]);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_time_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id,
                                      const grngo_vector *value) {
  grn_obj obj;
  GRN_TIME_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  for (i = 0; i < value->size; i++) {
    GRN_TIME_SET_AT(ctx, &obj, i, ((const int64_t *)value->ptr)[i]);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_float_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id,
                                       const grngo_vector *value) {
  grn_obj obj;
  GRN_FLOAT_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  for (i = 0; i < value->size; i++) {
    GRN_FLOAT_SET_AT(ctx, &obj, i, ((const double *)value->ptr)[i]);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_text_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id,
                                      const grngo_vector *value) {
  grn_obj obj;
  GRN_TEXT_INIT(&obj, GRN_OBJ_VECTOR);
  size_t i;
  const grngo_text *values = (const grngo_text *)value->ptr;
  for (i = 0; i < value->size; i++) {
    grn_vector_add_element(ctx, &obj, values[i].ptr, values[i].size,
                           0, obj.header.domain);
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_set_geo_point_vector(grn_ctx *ctx, grn_obj *column,
                                           grn_builtin_type data_type,
                                           grn_id id,
                                           const grngo_vector *value) {
  grn_obj obj;
  if (data_type == GRN_DB_TOKYO_GEO_POINT) {
    GRN_TOKYO_GEO_POINT_INIT(&obj, GRN_OBJ_VECTOR);
  } else {
    GRN_WGS84_GEO_POINT_INIT(&obj, GRN_OBJ_VECTOR);
  }
  size_t i;
  const grn_geo_point *values = (const grn_geo_point *)value->ptr;
  for (i = 0; i < value->size; i++) {
    grn_bulk_write(ctx, &obj, (const char *)&values[i], sizeof(values[i]));
  }
  grn_rc rc = grn_obj_set_value(ctx, column, id, &obj, GRN_OBJ_SET);
  GRN_OBJ_FIN(ctx, &obj);
  return rc == GRN_SUCCESS;
}

grn_bool grngo_column_get_bool(grn_ctx *ctx, grn_obj *column,
                               grn_id id, grn_bool *value) {
  grn_obj value_obj;
  GRN_BOOL_INIT(&value_obj, 0);
  grn_obj_get_value(ctx, column, id, &value_obj);
  *value = GRN_BOOL_VALUE(&value_obj);
  GRN_OBJ_FIN(ctx, &value_obj);
  return GRN_TRUE;
}

grn_bool grngo_column_get_int(grn_ctx *ctx, grn_obj *column,
                              grn_builtin_type data_type,
                              grn_id id, int64_t *value) {
  grn_obj value_obj;
  switch (data_type) {
    case GRN_DB_INT8: {
      GRN_INT8_INIT(&value_obj, 0);
      grn_obj_get_value(ctx, column, id, &value_obj);
      *value = GRN_INT8_VALUE(&value_obj);
      break;
    }
    case GRN_DB_INT16: {
      GRN_INT16_INIT(&value_obj, 0);
      grn_obj_get_value(ctx, column, id, &value_obj);
      *value = GRN_INT16_VALUE(&value_obj);
      break;
    }
    case GRN_DB_INT32: {
      GRN_INT32_INIT(&value_obj, 0);
      grn_obj_get_value(ctx, column, id, &value_obj);
      *value = GRN_INT32_VALUE(&value_obj);
      break;
    }
    case GRN_DB_INT64: {
      GRN_INT64_INIT(&value_obj, 0);
      grn_obj_get_value(ctx, column, id, &value_obj);
      *value = GRN_INT64_VALUE(&value_obj);
      break;
    }
    case GRN_DB_UINT8: {
      GRN_UINT8_INIT(&value_obj, 0);
      grn_obj_get_value(ctx, column, id, &value_obj);
      *value = GRN_UINT8_VALUE(&value_obj);
      break;
    }
    case GRN_DB_UINT16: {
      GRN_UINT16_INIT(&value_obj, 0);
      grn_obj_get_value(ctx, column, id, &value_obj);
      *value = GRN_UINT16_VALUE(&value_obj);
      break;
    }
    case GRN_DB_UINT32: {
      GRN_UINT32_INIT(&value_obj, 0);
      grn_obj_get_value(ctx, column, id, &value_obj);
      *value = GRN_UINT32_VALUE(&value_obj);
      break;
    }
    case GRN_DB_UINT64: {
      GRN_UINT64_INIT(&value_obj, 0);
      grn_obj_get_value(ctx, column, id, &value_obj);
      *value = GRN_UINT64_VALUE(&value_obj);
      break;
    }
    case GRN_DB_TIME: {
      GRN_TIME_INIT(&value_obj, 0);
      grn_obj_get_value(ctx, column, id, &value_obj);
      *value = GRN_TIME_VALUE(&value_obj);
      break;
    }
  }
  GRN_OBJ_FIN(ctx, &value_obj);
  return GRN_TRUE;
}

grn_bool grngo_column_get_float(grn_ctx *ctx, grn_obj *column,
                                grn_id id, double *value) {
  grn_obj value_obj;
  GRN_FLOAT_INIT(&value_obj, 0);
  grn_obj_get_value(ctx, column, id, &value_obj);
  *value = GRN_FLOAT_VALUE(&value_obj);
  GRN_OBJ_FIN(ctx, &value_obj);
  return GRN_TRUE;
}

grn_bool grngo_column_get_text(grn_ctx *ctx, grn_obj *column,
                               grn_id id, grngo_text *value) {
  grn_obj value_obj;
  GRN_TEXT_INIT(&value_obj, 0);
  grn_obj_get_value(ctx, column, id, &value_obj);
  size_t size = GRN_TEXT_LEN(&value_obj);
  if (size <= value->size) {
    memcpy(value->ptr, GRN_TEXT_VALUE(&value_obj), size);
  }
  value->size = size;
  GRN_OBJ_FIN(ctx, &value_obj);
  return GRN_TRUE;
}

grn_bool grngo_column_get_geo_point(grn_ctx *ctx, grn_obj *column,
                                    grn_id id, grn_geo_point *value) {
  grn_obj value_obj;
  GRN_WGS84_GEO_POINT_INIT(&value_obj, 0);
  grn_obj_get_value(ctx, column, id, &value_obj);
  GRN_GEO_POINT_VALUE(&value_obj, value->latitude, value->longitude);
  GRN_OBJ_FIN(ctx, &value_obj);
  return GRN_TRUE;
}

grn_bool grngo_column_get_bool_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id, grngo_vector *value) {
  grn_obj value_obj;
  GRN_BOOL_INIT(&value_obj, GRN_OBJ_VECTOR);
  grn_obj_get_value(ctx, column, id, &value_obj);
  size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
  size_t size = size_in_bytes / sizeof(grn_bool);
  if (size <= value->size) {
    memcpy(value->ptr, GRN_BULK_HEAD(&value_obj), size_in_bytes);
  }
  value->size = size;
  GRN_OBJ_FIN(ctx, &value_obj);
  return GRN_TRUE;
}

grn_bool grngo_column_get_int_vector(grn_ctx *ctx, grn_obj *column,
                                     grn_builtin_type data_type,
                                     grn_id id, grngo_vector *value) {
  grn_obj value_obj;
  switch (data_type) {
    case GRN_DB_INT8: {
      GRN_INT8_INIT(&value_obj, GRN_OBJ_VECTOR);
      grn_obj_get_value(ctx, column, id, &value_obj);
      size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
      size_t size = size_in_bytes / sizeof(int8_t);
      if (size <= value->size) {
        size_t i;
        for (i = 0; i < size; i++) {
          ((int64_t *)value->ptr)[i] = GRN_INT8_VALUE_AT(&value_obj, i);
        }
      }
      value->size = size;
      break;
    }
    case GRN_DB_INT16: {
      GRN_INT16_INIT(&value_obj, GRN_OBJ_VECTOR);
      grn_obj_get_value(ctx, column, id, &value_obj);
      size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
      size_t size = size_in_bytes / sizeof(int16_t);
      if (size <= value->size) {
        size_t i;
        for (i = 0; i < size; i++) {
          ((int64_t *)value->ptr)[i] = GRN_INT16_VALUE_AT(&value_obj, i);
        }
      }
      value->size = size;
      break;
    }
    case GRN_DB_INT32: {
      GRN_INT32_INIT(&value_obj, GRN_OBJ_VECTOR);
      grn_obj_get_value(ctx, column, id, &value_obj);
      size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
      size_t size = size_in_bytes / sizeof(int32_t);
      if (size <= value->size) {
        size_t i;
        for (i = 0; i < size; i++) {
          ((int64_t *)value->ptr)[i] = GRN_INT32_VALUE_AT(&value_obj, i);
        }
      }
      value->size = size;
      break;
    }
    case GRN_DB_INT64: {
      GRN_INT64_INIT(&value_obj, GRN_OBJ_VECTOR);
      grn_obj_get_value(ctx, column, id, &value_obj);
      size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
      size_t size = size_in_bytes / sizeof(int64_t);
      if (size <= value->size) {
        size_t i;
        for (i = 0; i < size; i++) {
          ((int64_t *)value->ptr)[i] = GRN_INT64_VALUE_AT(&value_obj, i);
        }
      }
      value->size = size;
      break;
    }
    case GRN_DB_UINT8: {
      GRN_UINT8_INIT(&value_obj, GRN_OBJ_VECTOR);
      grn_obj_get_value(ctx, column, id, &value_obj);
      size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
      size_t size = size_in_bytes / sizeof(uint8_t);
      if (size <= value->size) {
        size_t i;
        for (i = 0; i < size; i++) {
          ((int64_t *)value->ptr)[i] = GRN_UINT8_VALUE_AT(&value_obj, i);
        }
      }
      value->size = size;
      break;
    }
    case GRN_DB_UINT16: {
      GRN_UINT16_INIT(&value_obj, GRN_OBJ_VECTOR);
      grn_obj_get_value(ctx, column, id, &value_obj);
      size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
      size_t size = size_in_bytes / sizeof(uint16_t);
      if (size <= value->size) {
        size_t i;
        for (i = 0; i < size; i++) {
          ((int64_t *)value->ptr)[i] = GRN_UINT16_VALUE_AT(&value_obj, i);
        }
      }
      value->size = size;
      break;
    }
    case GRN_DB_UINT32: {
      GRN_UINT32_INIT(&value_obj, GRN_OBJ_VECTOR);
      grn_obj_get_value(ctx, column, id, &value_obj);
      size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
      size_t size = size_in_bytes / sizeof(uint32_t);
      if (size <= value->size) {
        size_t i;
        for (i = 0; i < size; i++) {
          ((int64_t *)value->ptr)[i] = GRN_UINT32_VALUE_AT(&value_obj, i);
        }
      }
      value->size = size;
      break;
    }
    case GRN_DB_UINT64: {
      GRN_UINT64_INIT(&value_obj, GRN_OBJ_VECTOR);
      grn_obj_get_value(ctx, column, id, &value_obj);
      size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
      size_t size = size_in_bytes / sizeof(uint64_t);
      if (size <= value->size) {
        size_t i;
        for (i = 0; i < size; i++) {
          ((int64_t *)value->ptr)[i] = GRN_UINT64_VALUE_AT(&value_obj, i);
        }
      }
      value->size = size;
      break;
    }
    case GRN_DB_TIME: {
      GRN_TIME_INIT(&value_obj, GRN_OBJ_VECTOR);
      grn_obj_get_value(ctx, column, id, &value_obj);
      size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
      size_t size = size_in_bytes / sizeof(int64_t);
      if (size <= value->size) {
        size_t i;
        for (i = 0; i < size; i++) {
          ((int64_t *)value->ptr)[i] = GRN_TIME_VALUE_AT(&value_obj, i);
        }
      }
      value->size = size;
      break;
    }
  }
  GRN_OBJ_FIN(ctx, &value_obj);
  return GRN_TRUE;
}

grn_bool grngo_column_get_float_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id, grngo_vector *value) {
  grn_obj value_obj;
  GRN_FLOAT_INIT(&value_obj, GRN_OBJ_VECTOR);
  grn_obj_get_value(ctx, column, id, &value_obj);
  size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
  size_t size = size_in_bytes / sizeof(double);
  if (size <= value->size) {
    memcpy(value->ptr, GRN_BULK_HEAD(&value_obj), size_in_bytes);
  }
  value->size = size;
  GRN_OBJ_FIN(ctx, &value_obj);
  return GRN_TRUE;
}

grn_bool grngo_column_get_text_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id, grngo_vector *value) {
  grn_obj value_obj;
  GRN_TEXT_INIT(&value_obj, GRN_OBJ_VECTOR);
  grn_obj_get_value(ctx, column, id, &value_obj);
  size_t size = grn_vector_size(ctx, &value_obj);
  if (size <= value->size) {
    size_t i;
    for (i = 0; i < size; i++) {
      // NOTE: grn_vector_get_element() assigns the address of the text body
      //       to text_ptr, but the body may be overwritten in the next call.
      const char *text_ptr;
      unsigned int text_size = grn_vector_get_element(ctx, &value_obj, i,
                                                      &text_ptr, NULL, NULL);
      grngo_text *text = &((grngo_text *)value->ptr)[i];
      if (text_size <= text->size) {
        memcpy(text->ptr, text_ptr, text_size);
      }
      text->size = text_size;
    }
  }
  value->size = size;
  GRN_OBJ_FIN(ctx, &value_obj);
  return GRN_TRUE;
}

grn_bool grngo_column_get_geo_point_vector(grn_ctx *ctx, grn_obj *column,
                                           grn_id id, grngo_vector *value) {
  grn_obj value_obj;
  GRN_WGS84_GEO_POINT_INIT(&value_obj, GRN_OBJ_VECTOR);
  grn_obj_get_value(ctx, column, id, &value_obj);
  size_t size_in_bytes = GRN_BULK_VSIZE(&value_obj);
  size_t size = size_in_bytes / sizeof(grn_geo_point);
  if (size <= value->size) {
    memcpy(value->ptr, GRN_BULK_HEAD(&value_obj), size_in_bytes);
  }
  value->size = size;
  GRN_OBJ_FIN(ctx, &value_obj);
  return GRN_TRUE;
}
