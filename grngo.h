#ifndef GRNGO_H
#define GRNGO_H

#include <stdint.h>
#include <stdlib.h>

#include <groonga.h>

#define GRNGO_ESTR_BUF_SIZE 256

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

// -- miscellaneous --

typedef struct {
  const char *ptr;
  size_t     size;
} grngo_text;

typedef struct {
  const void *ptr;
  size_t     size;
} grngo_vector;

// -- grngo_db --

typedef struct {
  grn_ctx *ctx;
  grn_obj *obj;
  char    *estr;  // TODO: Reserved.
  char    estr_buf[GRNGO_ESTR_BUF_SIZE];  // TODO: Reserved.
} grngo_db;

grn_rc grngo_create_db(const char *path, size_t path_len, grngo_db **db);
grn_rc grngo_open_db(const char *path, size_t path_len, grngo_db **db);
void grngo_close_db(grngo_db *db);

grn_rc grngo_send(grngo_db *db, const char *cmd, size_t cmd_len);
grn_rc grngo_recv(grngo_db *db, char **res, unsigned int *res_len);

// -- grngo_table --

typedef struct {
  grngo_db         *db;
  grn_obj          *obj;
  grn_builtin_type key_type;
} grngo_table;

grn_rc grngo_open_table(grngo_db *db, const char *name, size_t name_len,
                        grngo_table **tbl);
void grngo_close_table(grngo_table *tbl);

grn_rc grngo_insert_void(grngo_table *tbl, grn_bool *inserted, grn_id *id);
grn_rc grngo_insert_bool(grngo_table *tbl, grn_bool key,
                         grn_bool *inserted, grn_id *id);
grn_rc grngo_insert_int(grngo_table *tbl, int64_t key,
                        grn_bool *inserted, grn_id *id);
grn_rc grngo_insert_float(grngo_table *tbl, double key,
                          grn_bool *inserted, grn_id *id);
grn_rc grngo_insert_text(grngo_table *tbl, grngo_text key,
                         grn_bool *inserted, grn_id *id);
grn_rc grngo_insert_geo_point(grngo_table *tbl, grn_geo_point key,
                              grn_bool *inserted, grn_id *id);

// -- grngo_column --

typedef struct {
  grngo_db         *db;
  grngo_table      *table;
  grn_obj          **srcs;
  size_t           n_srcs;
  grn_obj          **src_bufs;
  grn_obj          *text_buf;
  grn_obj          *vector_buf;
  grn_builtin_type value_type;
  int              dimension;
  grn_bool         writable;
} grngo_column;

grn_rc grngo_open_column(grngo_table *tbl, const char *name, size_t name_len,
                         grngo_column **column);
void grngo_close_column(grngo_column *column);

grn_rc grngo_set_bool(grngo_column *column, grn_id id, grn_bool value);
grn_rc grngo_set_int(grngo_column *column, grn_id id, int64_t value);
grn_rc grngo_set_float(grngo_column *column, grn_id id, double value);
grn_rc grngo_set_text(grngo_column *column, grn_id id, grngo_text value);
grn_rc grngo_set_geo_point(grngo_column *column, grn_id id,
                           grn_geo_point value);
grn_rc grngo_set_bool_vector(grngo_column *column, grn_id id,
                             grngo_vector value);
grn_rc grngo_set_int_vector(grngo_column *column, grn_id id,
                            grngo_vector value);
grn_rc grngo_set_float_vector(grngo_column *column, grn_id id,
                              grngo_vector value);
grn_rc grngo_set_text_vector(grngo_column *column, grn_id id,
                             grngo_vector value);
grn_rc grngo_set_geo_point_vector(grngo_column *column, grn_id id,
                                  grngo_vector value);

grn_rc grngo_get(grngo_column *column, grn_id id, void **value);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  // GRNGO_H
