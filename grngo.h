#ifndef GRNGO_H
#define GRNGO_H

#include <stdint.h>
#include <stdlib.h>

#include <groonga.h>

typedef struct {
  char *ptr;
  size_t size;
} grngo_text;

typedef struct {
  void *ptr;
  size_t size;
} grngo_vector;

// grngo_find_table finds a table.
grn_rc grngo_find_table(grn_ctx *ctx, const char *name, size_t name_len,
                        grn_obj **table);

typedef struct {
  grn_builtin_type data_type;  // Data type (GRN_DB_VOID, GRN_DB_BOOL, etc.).
                               // If the type is table reference, the key type
                               // of the referenced table is stored.
  int              dimension;  // Vector depth, 0 means the type is scalar.
  grn_obj          *ref_table; // The referenced table of table reference.
} grngo_type_info;

// grngo_table_get_key_info() gets information of the table key.
grn_bool grngo_table_get_key_info(grn_ctx *ctx, grn_obj *table,
                                  grngo_type_info *key_info);
// grngo_table_get_value_info() gets information of the table value.
grn_bool grngo_table_get_value_info(grn_ctx *ctx, grn_obj *table,
                                    grngo_type_info *value_info);
// grngo_column_get_value_info() gets information of the column value.
grn_bool grngo_column_get_value_info(grn_ctx *ctx, grn_obj *column,
                                     grngo_type_info *value_info);

// grngo_table_get_name() returns the name of table.
// On success, a non-NULL pointer is returned and it must be freed by free().
// On failure, NULL is returned.
char *grngo_table_get_name(grn_ctx *ctx, grn_obj *table);

typedef struct {
  grn_id   id;       // Row ID, GRN_ID_NIL means the info is invalid.
  grn_bool inserted; // Inserted or not.
} grngo_row_info;

// grngo_table_insert_void() inserts an empty row.
grngo_row_info grngo_table_insert_void(grn_ctx *ctx, grn_obj *table);
// grngo_table_insert_bool() inserts a row with Bool key.
grngo_row_info grngo_table_insert_bool(grn_ctx *ctx, grn_obj *table,
                                       grn_bool key);
// grngo_table_insert_int*() inserts a row with Int key.
grngo_row_info grngo_table_insert_int8(grn_ctx *ctx, grn_obj *table,
                                       int8_t key);
grngo_row_info grngo_table_insert_int16(grn_ctx *ctx, grn_obj *table,
                                        int16_t key);
grngo_row_info grngo_table_insert_int32(grn_ctx *ctx, grn_obj *table,
                                        int32_t key);
grngo_row_info grngo_table_insert_int64(grn_ctx *ctx, grn_obj *table,
                                        int64_t key);
grngo_row_info grngo_table_insert_uint8(grn_ctx *ctx, grn_obj *table,
                                        uint8_t key);
grngo_row_info grngo_table_insert_uint16(grn_ctx *ctx, grn_obj *table,
                                         uint16_t key);
grngo_row_info grngo_table_insert_uint32(grn_ctx *ctx, grn_obj *table,
                                         uint32_t key);
grngo_row_info grngo_table_insert_uint64(grn_ctx *ctx, grn_obj *table,
                                         uint64_t key);
grngo_row_info grngo_table_insert_time(grn_ctx *ctx, grn_obj *table,
                                       int64_t key);
// grngo_table_insert_float() inserts a row with Float key.
grngo_row_info grngo_table_insert_float(grn_ctx *ctx, grn_obj *table,
                                        double key);
// grngo_table_insert_text() inserts a row with Text key.
grngo_row_info grngo_table_insert_text(grn_ctx *ctx, grn_obj *table,
                                       const grngo_text *key);
// grngo_table_insert_geo_point() inserts a row with GeoPoint key.
grngo_row_info grngo_table_insert_geo_point(grn_ctx *ctx, grn_obj *table,
                                            grn_geo_point key);

// grngo_column_set_bool() assigns a Bool value.
grn_bool grngo_column_set_bool(grn_ctx *ctx, grn_obj *column,
                               grn_id id, grn_bool value);
// grngo_column_set_int() assigns an Int value.
grn_bool grngo_column_set_int8(grn_ctx *ctx, grn_obj *column,
                               grn_id id, int8_t value);
grn_bool grngo_column_set_int16(grn_ctx *ctx, grn_obj *column,
                                grn_id id, int16_t value);
grn_bool grngo_column_set_int32(grn_ctx *ctx, grn_obj *column,
                                grn_id id, int32_t value);
grn_bool grngo_column_set_int64(grn_ctx *ctx, grn_obj *column,
                                grn_id id, int64_t value);
grn_bool grngo_column_set_uint8(grn_ctx *ctx, grn_obj *column,
                                grn_id id, uint8_t value);
grn_bool grngo_column_set_uint16(grn_ctx *ctx, grn_obj *column,
                                 grn_id id, uint16_t value);
grn_bool grngo_column_set_uint32(grn_ctx *ctx, grn_obj *column,
                                 grn_id id, uint32_t value);
grn_bool grngo_column_set_uint64(grn_ctx *ctx, grn_obj *column,
                                 grn_id id, uint64_t value);
grn_bool grngo_column_set_time(grn_ctx *ctx, grn_obj *column,
                               grn_id id, int64_t value);
// grngo_column_set_float() assigns a Float value.
grn_bool grngo_column_set_float(grn_ctx *ctx, grn_obj *column,
                                grn_id id, double value);
// grngo_column_set_text() assigns a Text value.
grn_bool grngo_column_set_text(grn_ctx *ctx, grn_obj *column,
                               grn_id id, const grngo_text *value);
// grngo_column_set_geo_point() assigns a GeoPoint value.
grn_bool grngo_column_set_geo_point(grn_ctx *ctx, grn_obj *column,
                                    grn_builtin_type data_type,
                                    grn_id id, grn_geo_point value);
// grngo_column_set_bool_vector() assigns a Bool vector.
grn_bool grngo_column_set_bool_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id,
                                      const grngo_vector *value);
// grngo_column_set_int_vector() assigns an Int vector.
grn_bool grngo_column_set_int8_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id,
                                      const grngo_vector *value);
grn_bool grngo_column_set_int16_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id,
                                       const grngo_vector *value);
grn_bool grngo_column_set_int32_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id,
                                       const grngo_vector *value);
grn_bool grngo_column_set_int64_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id,
                                       const grngo_vector *value);
grn_bool grngo_column_set_uint8_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id,
                                       const grngo_vector *value);
grn_bool grngo_column_set_uint16_vector(grn_ctx *ctx, grn_obj *column,
                                        grn_id id,
                                        const grngo_vector *value);
grn_bool grngo_column_set_uint32_vector(grn_ctx *ctx, grn_obj *column,
                                        grn_id id,
                                        const grngo_vector *value);
grn_bool grngo_column_set_uint64_vector(grn_ctx *ctx, grn_obj *column,
                                        grn_id id,
                                        const grngo_vector *value);
grn_bool grngo_column_set_time_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id,
                                      const grngo_vector *value);
// grngo_column_set_float_vector() assigns a Float vector.
grn_bool grngo_column_set_float_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id,
                                       const grngo_vector *value);
// grngo_column_set_text_vector() assigns a Text vector.
// value must refer to an array of grngo_text.
grn_bool grngo_column_set_text_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id,
                                      const grngo_vector *value);
// grngo_column_set_geo_point_vector() assigns a GeoPoint vector.
grn_bool grngo_column_set_geo_point_vector(grn_ctx *ctx, grn_obj *column,
                                           grn_builtin_type data_type,
                                           grn_id id,
                                           const grngo_vector *value);

// grngo_column_get_X_vector() sets *(X *)(value.ptr)[i] if value->size >=
// the actual vector size.
// In the case of Text, bodies are copied to (X *)(value.ptr)[i].ptr if
// (X *)(value.ptr)[i].size >= the actual body size.
// Then, grngo_column_get_X_vector() sets value->size.

// grngo_column_get_bool() gets a stored Bool value.
grn_bool grngo_column_get_bool(grn_ctx *ctx, grn_obj *column,
                               grn_id id, grn_bool *value);
// grngo_column_get_int() gets a stored Int value.
grn_bool grngo_column_get_int(grn_ctx *ctx, grn_obj *column,
                              grn_builtin_type data_type,
                              grn_id id, int64_t *value);
// grngo_column_get_float() gets a stored Float value.
grn_bool grngo_column_get_float(grn_ctx *ctx, grn_obj *column,
                                grn_id id, double *value);
// grngo_column_get_text() gets a stored Text value.
grn_bool grngo_column_get_text(grn_ctx *ctx, grn_obj *column,
                               grn_id id, grngo_text *value);
// grngo_column_get_geo_point() gets a stored GeoPoint value.
grn_bool grngo_column_get_geo_point(grn_ctx *ctx, grn_obj *column,
                                    grn_id id, grn_geo_point *value);
// grngo_column_get_bool_vector() gets a stored Bool vector.
grn_bool grngo_column_get_bool_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id, grngo_vector *value);
// grngo_column_get_int_vector() gets a stored Int vector.
grn_bool grngo_column_get_int_vector(grn_ctx *ctx, grn_obj *column,
                                     grn_builtin_type data_type,
                                     grn_id id, grngo_vector *value);
// grngo_column_get_float_vector() gets a stored Float vector.
grn_bool grngo_column_get_float_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id, grngo_vector *value);
// grngo_column_get_text_vector() gets a stored Text vector.
// value must refer to an array of grngo_text.
grn_bool grngo_column_get_text_vector(grn_ctx *ctx, grn_obj *column,
                                      grn_id id, grngo_vector *value);
// grngo_column_get_geo_point_vector() gets a stored GeoPoint vector.
grn_bool grngo_column_get_geo_point_vector(grn_ctx *ctx, grn_obj *column,
                                           grn_id id, grngo_vector *value);

#endif  // GRNGO_H
