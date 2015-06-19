#ifndef GRN_CGO_H
#define GRN_CGO_H

#include <stdint.h>
#include <stdlib.h>

#include <groonga.h>

typedef struct {
  char *ptr;
  size_t size;
} grn_cgo_text;

typedef struct {
  void *ptr;
  size_t size;
} grn_cgo_vector;

// grn_cgo_find_table() finds a table with the given name.
// If found, an object associated with the table is returned.
// If not found, NULL is returned.
grn_obj *grn_cgo_find_table(grn_ctx *ctx, const char *name, int name_len);

typedef struct {
  grn_id  data_type;  // Data type (GRN_DB_VOID, GRN_DB_BOOL, etc.).
                      // If the type is table reference, the key type of the
                      // referenced table is stored.
  int     dimension;  // Vector depth, 0 means the type is scalar.
  grn_obj *ref_table; // The referenced table of table reference.
} grn_cgo_type_info;

// grn_cgo_table_get_key_info() gets information of the table key.
grn_bool grn_cgo_table_get_key_info(grn_ctx *ctx, grn_obj *table,
                                    grn_cgo_type_info *key_info);
// grn_cgo_table_get_value_info() gets information of the table value.
grn_bool grn_cgo_table_get_value_info(grn_ctx *ctx, grn_obj *table,
                                      grn_cgo_type_info *value_info);
// grn_cgo_column_get_value_info() gets information of the column value.
grn_bool grn_cgo_column_get_value_info(grn_ctx *ctx, grn_obj *column,
                                       grn_cgo_type_info *value_info);

// grn_cgo_table_get_name() returns the name of table.
// On success, a non-NULL pointer is returned and it must be freed by free().
// On failure, NULL is returned.
char *grn_cgo_table_get_name(grn_ctx *ctx, grn_obj *table);

typedef struct {
  grn_id   id;       // Row ID, GRN_ID_NIL means the info is invalid.
  grn_bool inserted; // Inserted or not.
} grn_cgo_row_info;

// grn_cgo_table_insert_void() inserts an empty row.
grn_cgo_row_info grn_cgo_table_insert_void(grn_ctx *ctx, grn_obj *table);
// grn_cgo_table_insert_bool() inserts a row with Bool key.
grn_cgo_row_info grn_cgo_table_insert_bool(grn_ctx *ctx, grn_obj *table,
                                           grn_bool key);
// grn_cgo_table_insert_int() inserts a row with Int key.
grn_cgo_row_info grn_cgo_table_insert_int(grn_ctx *ctx, grn_obj *table,
                                          int64_t key);
// grn_cgo_table_insert_float() inserts a row with Float key.
grn_cgo_row_info grn_cgo_table_insert_float(grn_ctx *ctx, grn_obj *table,
                                            double key);
// grn_cgo_table_insert_geo_point() inserts a row with GeoPoint key.
grn_cgo_row_info grn_cgo_table_insert_geo_point(grn_ctx *ctx, grn_obj *table,
                                                grn_geo_point key);
// grn_cgo_table_insert_text() inserts a row with Text key.
grn_cgo_row_info grn_cgo_table_insert_text(grn_ctx *ctx, grn_obj *table,
                                           const grn_cgo_text *key);

// grn_cgo_column_set_bool() assigns a Bool value.
grn_bool grn_cgo_column_set_bool(grn_ctx *ctx, grn_obj *column,
                                 grn_id id, grn_bool value);
// grn_cgo_column_set_int() assigns an Int value.
grn_bool grn_cgo_column_set_int(grn_ctx *ctx, grn_obj *column,
                                grn_id id, int64_t value);
// grn_cgo_column_set_float() assigns a Float value.
grn_bool grn_cgo_column_set_float(grn_ctx *ctx, grn_obj *column,
                                  grn_id id, double value);
// grn_cgo_column_set_geo_point() assigns a GeoPoint value.
grn_bool grn_cgo_column_set_geo_point(grn_ctx *ctx, grn_obj *column,
                                      grn_id id, grn_geo_point value);
// grn_cgo_column_set_text() assigns a Text value.
grn_bool grn_cgo_column_set_text(grn_ctx *ctx, grn_obj *column,
                                 grn_id id, const grn_cgo_text *value);
// grn_cgo_column_set_bool_vector() assigns a Bool vector.
grn_bool grn_cgo_column_set_bool_vector(grn_ctx *ctx, grn_obj *column,
                                        grn_id id,
                                        const grn_cgo_vector *value);
// grn_cgo_column_set_int_vector() assigns an Int vector.
grn_bool grn_cgo_column_set_int_vector(grn_ctx *ctx, grn_obj *column,
                                       grn_id id,
                                       const grn_cgo_vector *value);
// grn_cgo_column_set_float_vector() assigns a Float vector.
grn_bool grn_cgo_column_set_float_vector(grn_ctx *ctx, grn_obj *column,
                                         grn_id id,
                                         const grn_cgo_vector *value);
// grn_cgo_column_set_geo_point_vector() assigns a GeoPoint vector.
grn_bool grn_cgo_column_set_geo_point_vector(grn_ctx *ctx, grn_obj *column,
                                             grn_id id,
                                             const grn_cgo_vector *value);
// grn_cgo_column_set_text_vector() assigns a Text vector.
// value must refer to an array of grn_cgo_text.
grn_bool grn_cgo_column_set_text_vector(grn_ctx *ctx, grn_obj *column,
                                        grn_id id,
                                        const grn_cgo_vector *value);

// grn_cgo_column_get_X_vector() sets *(X *)(value.ptr)[i] if value->size >=
// the actual vector size.
// In the case of Text, bodies are copied to (X *)(value.ptr)[i].ptr if
// (X *)(value.ptr)[i].size >= the actual body size.
// Then, grn_cgo_column_get_X_vector() sets value->size.

// grn_cgo_column_get_bool() gets a stored Bool value.
grn_bool grn_cgo_column_get_bool(grn_ctx *ctx, grn_obj *column,
                                 grn_id id, grn_bool *value);
// grn_cgo_column_get_int() gets a stored Int value.
grn_bool grn_cgo_column_get_int(grn_ctx *ctx, grn_obj *column,
                                grn_id id, int64_t *value);
// grn_cgo_column_get_float() gets a stored Float value.
grn_bool grn_cgo_column_get_float(grn_ctx *ctx, grn_obj *column,
                                  grn_id id, double *value);
// grn_cgo_column_get_geo_point() gets a stored GeoPoint value.
grn_bool grn_cgo_column_get_geo_point(grn_ctx *ctx, grn_obj *column,
                                      grn_id id, grn_geo_point *value);
// grn_cgo_column_get_text() gets a stored Text value.
grn_bool grn_cgo_column_get_text(grn_ctx *ctx, grn_obj *column,
                                 grn_id id, grn_cgo_text *value);
// grn_cgo_column_get_bool_vector() gets a stored Bool vector.
grn_bool grn_cgo_column_get_bool_vector(grn_ctx *ctx, grn_obj *column,
                                        grn_id id, grn_cgo_vector *value);
// grn_cgo_column_get_int_vector() gets a stored Int vector.
grn_bool grn_cgo_column_get_int_vector(grn_ctx *ctx, grn_obj *column,
                                        grn_id id, grn_cgo_vector *value);
// grn_cgo_column_get_float_vector() gets a stored Float vector.
grn_bool grn_cgo_column_get_float_vector(grn_ctx *ctx, grn_obj *column,
                                         grn_id id, grn_cgo_vector *value);
// grn_cgo_column_get_geo_point_vector() gets a stored GeoPoint vector.
grn_bool grn_cgo_column_get_geo_point_vector(grn_ctx *ctx, grn_obj *column,
                                             grn_id id, grn_cgo_vector *value);
// grn_cgo_column_get_text_vector() gets a stored Text vector.
// value must refer to an array of grn_cgo_text.
grn_bool grn_cgo_column_get_text_vector(grn_ctx *ctx, grn_obj *column,
                                        grn_id id, grn_cgo_vector *value);

grn_bool grn_cgo_column_get_bools(grn_ctx *ctx, grn_obj *column, size_t n,
                                  const int64_t *ids, grn_bool *values);

#endif  // GRN_CGO_H
