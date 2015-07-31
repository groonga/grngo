#include "grngo.h"

#include <math.h>
#include <string.h>

// -- debug --

#include <stdio.h>

#define GRNGO_DEBUG(fmt, ...)\
  fprintf(stderr, "%s:%d: In %s: " fmt "\n",\
          __FILE__, __LINE__, __PRETTY_FUNCTION__, __VA_ARGS__)

// -- miscellaneous --

#define GRNGO_MAX_BUILTIN_TYPE_ID GRN_DB_WGS84_GEO_POINT

#define GRNGO_MAX_SHORT_TEXT_LEN 4095
#define GRNGO_MAX_TEXT_LEN       65535
#define GRNGO_MAX_LONG_TEXT_LEN  2147484647

#define GRNGO_BOOL_DB_TYPE            grn_bool
#define GRNGO_INT8_DB_TYPE            int8_t
#define GRNGO_INT16_DB_TYPE           int16_t
#define GRNGO_INT32_DB_TYPE           int32_t
#define GRNGO_INT64_DB_TYPE           int64_t
#define GRNGO_UINT8_DB_TYPE           uint8_t
#define GRNGO_UINT16_DB_TYPE          uint16_t
#define GRNGO_UINT32_DB_TYPE          uint32_t
#define GRNGO_UINT64_DB_TYPE          uint64_t
#define GRNGO_FLOAT_DB_TYPE           double
#define GRNGO_TIME_DB_TYPE            int64_t
#define GRNGO_TEXT_DB_TYPE            grngo_text
#define GRNGO_TOKYO_GEO_POINT_DB_TYPE grn_geo_point
#define GRNGO_WGS84_GEO_POINT_DB_TYPE grn_geo_point

#define GRNGO_DB_TYPE(type) GRNGO_ ## type ## _DB_TYPE

#define GRNGO_BOOL_C_TYPE            grn_bool
#define GRNGO_INT8_C_TYPE            int64_t
#define GRNGO_INT16_C_TYPE           int64_t
#define GRNGO_INT32_C_TYPE           int64_t
#define GRNGO_INT64_C_TYPE           int64_t
#define GRNGO_UINT8_C_TYPE           int64_t
#define GRNGO_UINT16_C_TYPE          int64_t
#define GRNGO_UINT32_C_TYPE          int64_t
#define GRNGO_UINT64_C_TYPE          int64_t
#define GRNGO_FLOAT_C_TYPE           double
#define GRNGO_TIME_C_TYPE            int64_t
#define GRNGO_TEXT_C_TYPE            grngo_text
#define GRNGO_TOKYO_GEO_POINT_C_TYPE grn_geo_point
#define GRNGO_WGS84_GEO_POINT_C_TYPE grn_geo_point

#define GRNGO_C_TYPE(type)  GRNGO_ ## type ## _C_TYPE

#define GRNGO_TEST_BOOL(value)       (1)
#define GRNGO_TEST_INT8(value)       \
  (((value) >= INT8_MIN) && ((value) <= INT8_MAX))
#define GRNGO_TEST_INT16(value)      \
  (((value) >= INT16_MIN) && ((value) <= INT16_MAX))
#define GRNGO_TEST_INT32(value)      \
  (((value) >= INT32_MIN) && ((value) <= INT32_MAX))
#define GRNGO_TEST_INT64(value)      (1)
#define GRNGO_TEST_UINT8(value)      \
  (((value) >= 0) && ((value) <= (int64_t)UINT8_MAX))
#define GRNGO_TEST_UINT16(value)     \
  (((value) >= 0) && ((value) <= (int64_t)UINT16_MAX))
#define GRNGO_TEST_UINT32(value)     \
  (((value) >= 0) && ((value) <= (int64_t)UINT32_MAX))
#define GRNGO_TEST_UINT64(value)     ((value) >= 0)
#define GRNGO_TEST_TIME(value)       (1)
#define GRNGO_TEST_FLOAT(value)      (!isnan(value))
#define GRNGO_TEST_SHORT_TEXT(value) \
  (((value).ptr && ((value).size < GRNGO_MAX_SHORT_TEXT_LEN)) ||\
   (!(value).ptr && !(value).size))
#define GRNGO_TEST_TEXT(value)       \
  (((value).ptr && ((value).size < GRNGO_MAX_TEXT_LEN)) ||\
   (!(value).ptr && !(value).size))
#define GRNGO_TEST_LONG_TEXT(value)  \
  (((value).ptr && ((value).size < GRNGO_MAX_LONG_TEXT_LEN)) ||\
   (!(value).ptr && !(value).size))
#define GRNGO_TEST_GEO_POINT(value)  \
  (((value).latitude  >= ( -90 * 60 * 60 * 1000)) &&\
   ((value).latitude  <= (  90 * 60 * 60 * 1000)) &&\
   ((value).longitude >= (-180 * 60 * 60 * 1000)) &&\
   ((value).longitude <= ( 180 * 60 * 60 * 1000)))
#define GRNGO_TEST_VECTOR(value)     \
  ((value.ptr) || (!(value).ptr && !(value).size))

static void *
_grngo_malloc(grngo_db *db, size_t size,
              const char *file, int line, const char *func) {
  void *buf = malloc(size);
  if (!buf && db) {
    // TODO: Error!
  }
  return buf;
}
#define GRNGO_MALLOC(db, size)\
  _grngo_malloc(db, size, __FILE__, __LINE__, __PRETTY_FUNCTION__)

static void *
_grngo_realloc(grngo_db *db, void *ptr, size_t size,
               const char *file, int line, const char *func) {
  void *buf = realloc(ptr, size);
  if (!buf && db) {
    // TODO: Error!
  }
  return buf;
}
#define GRNGO_REALLOC(db, ptr, size)\
  _grngo_realloc(db, ptr, size, __FILE__, __LINE__, __PRETTY_FUNCTION__)

static void
_grngo_free(grngo_db *db, void *buf,
            const char *file, int line, const char *func) {
  free(buf);
}
#define GRNGO_FREE(db, buf)\
  _grngo_free(db, buf, __FILE__, __LINE__, __PRETTY_FUNCTION__)

static grn_bool
_grngo_is_vector(grn_obj *obj) {
  if (obj->header.type != GRN_COLUMN_VAR_SIZE) {
    return GRN_FALSE;
  }
  grn_obj_flags type = obj->header.flags & GRN_OBJ_COLUMN_TYPE_MASK;
  return type == GRN_OBJ_COLUMN_VECTOR;
}

// -- grngo_db --

static grngo_db *
_grngo_new_db(void) {
  grngo_db *db = (grngo_db *)GRNGO_MALLOC(NULL, sizeof(*db));
  if (!db) {
    return NULL;
  }
  memset(db, 0, sizeof(*db));
  db->ctx = NULL;
  db->obj = NULL;
  db->estr = db->estr_buf;
  return db;
}

static void
_grngo_delete_db(grngo_db *db) {
  if (db->obj) {
    grn_obj_close(db->ctx, db->obj);
  }
  if (db->ctx) {
    grn_ctx_close(db->ctx);
  }
  GRNGO_FREE(NULL, db);
}

static grn_rc
_grngo_create_db(grngo_db *db, const char *path) {
  db->ctx = grn_ctx_open(0);
  if (!db->ctx) {
    return GRN_NO_MEMORY_AVAILABLE;
  }
  db->obj = grn_db_create(db->ctx, path, NULL);
  if (!db->obj) {
    if (db->ctx->rc != GRN_SUCCESS) {
      return db->ctx->rc;
    }
    return GRN_UNKNOWN_ERROR;
  }
  return GRN_SUCCESS;
}

static grn_rc
_grngo_open_db(grngo_db *db, const char *path) {
  db->ctx = grn_ctx_open(0);
  if (!db->ctx) {
    return GRN_NO_MEMORY_AVAILABLE;
  }
  db->obj = grn_db_open(db->ctx, path);
  if (!db->obj) {
    if (db->ctx->rc != GRN_SUCCESS) {
      return db->ctx->rc;
    }
    return GRN_UNKNOWN_ERROR;
  }
  return GRN_SUCCESS;
}

grn_rc
grngo_create_db(const char *path, size_t path_len, grngo_db **db) {
  if ((!path && path_len) || !db) {
    return GRN_INVALID_ARGUMENT;
  }
  // Create a zero-terminated path.
  char *path_cstr = NULL;
  if (path) {
    path_cstr = GRNGO_MALLOC(NULL, path_len + 1);
    if (!path_cstr) {
      return GRN_NO_MEMORY_AVAILABLE;
    }
    memcpy(path_cstr, path, path_len);
    path_cstr[path_len] = '\0';
  }
  // Create a DB.
  grngo_db *new_db = _grngo_new_db();
  grn_rc rc = new_db ? GRN_SUCCESS : GRN_NO_MEMORY_AVAILABLE;
  if (rc == GRN_SUCCESS) {
    rc = _grngo_create_db(new_db, path_cstr);
    if (rc == GRN_SUCCESS) {
      *db = new_db;
    } else {
      _grngo_delete_db(new_db);
    }
  }
  GRNGO_FREE(NULL, path_cstr);
  return rc;
}

grn_rc
grngo_open_db(const char *path, size_t path_len, grngo_db **db) {
  if ((!path && path_len) || !db) {
    return GRN_INVALID_ARGUMENT;
  }
  // Create a zero-terminated path.
  char *path_cstr = NULL;
  if (path) {
    path_cstr = GRNGO_MALLOC(NULL, path_len + 1);
    if (!path_cstr) {
      return GRN_NO_MEMORY_AVAILABLE;
    }
    memcpy(path_cstr, path, path_len);
    path_cstr[path_len] = '\0';
  }
  // Open a DB.
  grngo_db *new_db = _grngo_new_db();
  grn_rc rc = new_db ? GRN_SUCCESS : GRN_NO_MEMORY_AVAILABLE;
  if (rc == GRN_SUCCESS) {
    rc = _grngo_open_db(new_db, path_cstr);
    if (rc == GRN_SUCCESS) {
      *db = new_db;
    } else {
      _grngo_delete_db(new_db);
    }
  }
  GRNGO_FREE(NULL, path_cstr);
  return rc;
}

void
grngo_close_db(grngo_db *db) {
  if (db) {
    _grngo_delete_db(db);
  }
}

grn_rc
grngo_send(grngo_db *db, const char *cmd, size_t cmd_len) {
  if (!db || (!cmd && cmd_len)) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_rc rc = grn_ctx_send(db->ctx, cmd, cmd_len, 0);
  if (rc != GRN_SUCCESS) {
    return rc;
  }
  return db->ctx->rc;
}

grn_rc
grngo_recv(grngo_db *db, char **res, unsigned int *res_len) {
  if (!db || !res || !res_len) {
    return GRN_INVALID_ARGUMENT;
  }
  int flags;
  grn_rc rc = grn_ctx_recv(db->ctx, res, res_len, &flags);
  if (rc != GRN_SUCCESS) {
    return rc;
  }
  return db->ctx->rc;
}

// -- grngo_table --

static grngo_table *
_grngo_new_table(grngo_db *db) {
  grngo_table *table = (grngo_table *)GRNGO_MALLOC(db, sizeof(*table));
  if (!table) {
    return NULL;
  }
  memset(table, 0, sizeof(*table));
  table->db = db;
  table->objs = NULL;
  return table;
}

static void
_grngo_delete_table(grngo_table *table) {
  if (table->objs) {
    size_t i;
    for (i = 0; i < table->n_objs; i++) {
      if (table->objs[i]) {
        grn_obj_unlink(table->db->ctx, table->objs[i]);
      }
    }
    GRNGO_FREE(table->db, table->objs);
  }
  GRNGO_FREE(table->db, table);
}

static grn_rc
_grngo_open_table(grngo_table *table, const char *name, size_t name_len) {
  grn_ctx *ctx = table->db->ctx;
  grn_obj *obj = grn_ctx_get(ctx, name, name_len);
  while (obj) {
    // Register an object.
    size_t new_size = sizeof(grn_obj *) * (table->n_objs + 1);
    grn_obj **new_objs = (grn_obj **)GRNGO_REALLOC(table->db, table->objs,
                                                   new_size);
    if (!new_objs) {
      grn_obj_unlink(ctx, obj);
      return GRN_NO_MEMORY_AVAILABLE;
    }
    table->objs = new_objs;
    table->objs[table->n_objs] = obj;
    table->n_objs++;

    // Detect the builtin type of _key or dereference _key.
    grn_id domain = obj->header.domain;
    if (obj->header.type == GRN_TABLE_NO_KEY) {
      domain = GRN_DB_VOID;
    }
    if (domain <= GRNGO_MAX_BUILTIN_TYPE_ID) {
      table->key_type = domain;
      return GRN_SUCCESS;
    }
    obj = grn_ctx_at(ctx, domain);
  }
  if (!obj) {
    if (ctx->rc != GRN_SUCCESS) {
      return ctx->rc;
    }
    return GRN_UNKNOWN_ERROR;
  }
  return GRN_SUCCESS;
}

grn_rc
grngo_open_table(grngo_db *db, const char *name, size_t name_len,
                 grngo_table **table) {
  if (!db || !name || (name_len == 0) || !table) {
    return GRN_INVALID_ARGUMENT;
  }
  grngo_table *new_table = _grngo_new_table(db);
  grn_rc rc = new_table ? GRN_SUCCESS : GRN_NO_MEMORY_AVAILABLE;
  if (rc == GRN_SUCCESS) {
    rc = _grngo_open_table(new_table, name, name_len);
    if (rc == GRN_SUCCESS) {
      *table = new_table;
    } else {
      _grngo_delete_table(new_table);
    }
  }
  return rc;
}

void
grngo_close_table(grngo_table *table) {
  if (table) {
    _grngo_delete_table(table);
  }
}

static grn_rc
_grngo_insert_row(grngo_table *table, const void *key, size_t key_size,
                  grn_bool *inserted, grn_id *id) {
  grn_ctx *ctx = table->db->ctx;
  size_t i = table->n_objs - 1;
  int tmp_inserted;
  grn_id tmp_id = grn_table_add(ctx, table->objs[i], key, key_size,
                                &tmp_inserted);
  if (tmp_id == GRN_ID_NIL) {
    if (ctx->rc != GRN_SUCCESS) {
      return ctx->rc;
    }
    return GRN_UNKNOWN_ERROR;
  }
  // Resolve table references.
  while (i > 0) {
    i--;
    tmp_id = grn_table_add(ctx, table->objs[i], &tmp_id, sizeof(tmp_id),
                           &tmp_inserted);
    if (tmp_id == GRN_ID_NIL) {
      if (ctx->rc != GRN_SUCCESS) {
        return ctx->rc;
      }
      return GRN_UNKNOWN_ERROR;
    }
  }
  *inserted = (grn_bool)tmp_inserted;
  *id = tmp_id;
  return GRN_SUCCESS;
}

grn_rc
grngo_insert_void(grngo_table *table, grn_bool *inserted, grn_id *id) {
  if (!table || !inserted || !id) {
    return GRN_INVALID_ARGUMENT;
  }
  if (table->key_type != GRN_DB_VOID) {
    return GRN_INVALID_ARGUMENT;
  }
  return _grngo_insert_row(table, NULL, 0, inserted, id);
}

grn_rc
grngo_insert_bool(grngo_table *table, grn_bool key,
                  grn_bool *inserted, grn_id *id) {
  if (!table || !inserted || !id) {
    return GRN_INVALID_ARGUMENT;
  }
  if ((table->key_type != GRN_DB_BOOL) || !GRNGO_TEST_BOOL(key)) {
    return GRN_INVALID_ARGUMENT;
  }
  return _grngo_insert_row(table, &key, sizeof(key), inserted, id);
}

#define GRNGO_INSERT_INT_CASE_BLOCK(type)\
  case GRN_DB_ ## type: {\
    if (!GRNGO_TEST_ ## type(key)) {\
      return GRN_INVALID_ARGUMENT;\
    }\
    GRNGO_DB_TYPE(type) tmp_key = (GRNGO_DB_TYPE(type))key;\
    return _grngo_insert_row(table, &tmp_key, sizeof(tmp_key), inserted, id);\
  }
grn_rc
grngo_insert_int(grngo_table *table, int64_t key,
                 grn_bool *inserted, grn_id *id) {
  if (!table || !inserted || !id) {
    return GRN_INVALID_ARGUMENT;
  }
  switch (table->key_type) {
    GRNGO_INSERT_INT_CASE_BLOCK(INT8)
    GRNGO_INSERT_INT_CASE_BLOCK(INT16)
    GRNGO_INSERT_INT_CASE_BLOCK(INT32)
    GRNGO_INSERT_INT_CASE_BLOCK(INT64)
    GRNGO_INSERT_INT_CASE_BLOCK(UINT8)
    GRNGO_INSERT_INT_CASE_BLOCK(UINT16)
    GRNGO_INSERT_INT_CASE_BLOCK(UINT32)
    GRNGO_INSERT_INT_CASE_BLOCK(UINT64)
    GRNGO_INSERT_INT_CASE_BLOCK(TIME)
    default: {
      return GRN_INVALID_ARGUMENT;
    }
  }
}
#undef GRNGO_INSERT_INT_CASE_BLOCK

grn_rc
grngo_insert_float(grngo_table *table, double key,
                   grn_bool *inserted, grn_id *id) {
  if (!table || !inserted || !id) {
    return GRN_INVALID_ARGUMENT;
  }
  if ((table->key_type != GRN_DB_FLOAT) || !GRNGO_TEST_FLOAT(key)) {
    return GRN_INVALID_ARGUMENT;
  }
  return _grngo_insert_row(table, &key, sizeof(key), inserted, id);
}

grn_rc
grngo_insert_text(grngo_table *table, grngo_text key,
                  grn_bool *inserted, grn_id *id) {
  if (!table || !inserted || !id) {
    return GRN_INVALID_ARGUMENT;
  }
  if ((table->key_type != GRN_DB_SHORT_TEXT) || !GRNGO_TEST_SHORT_TEXT(key)) {
    return GRN_INVALID_ARGUMENT;
  }
  return _grngo_insert_row(table, key.ptr, key.size, inserted, id);
}

grn_rc
grngo_insert_geo_point(grngo_table *table, grn_geo_point key,
                       grn_bool *inserted, grn_id *id) {
  if (!table || !inserted || !id) {
    return GRN_INVALID_ARGUMENT;
  }
  switch (table->key_type) {
    case GRN_DB_TOKYO_GEO_POINT:
    case GRN_DB_WGS84_GEO_POINT: {
      if (!GRNGO_TEST_GEO_POINT(key)) {
        return GRN_INVALID_ARGUMENT;
      }
      break;
    }
    default: {
      return GRN_INVALID_ARGUMENT;
    }
  }
  return _grngo_insert_row(table, &key, sizeof(key), inserted, id);
}

// -- grngo_column --

static grngo_column *
_grngo_new_column(grngo_table *table) {
  grngo_column *column = (grngo_column *)GRNGO_MALLOC(table->db,
                                                      sizeof(*column));
  if (!column) {
    return NULL;
  }
  memset(column, 0, sizeof(*column));
  column->db = table->db;
  column->table = table;
  column->srcs = NULL;
  column->src_bufs = NULL;
  column->text_buf = NULL;
  column->vector_buf = NULL;
  return column;
}

static void
_grngo_delete_column(grngo_column *column) {
  grn_ctx *ctx = column->db->ctx;
  if (column->srcs) {
    size_t i;
    for (i = 0; i < column->n_srcs; i++) {
      grn_obj_unlink(ctx, column->srcs[i]);
    }
    GRNGO_FREE(column->db, column->srcs);
  }
  if (column->src_bufs) {
    size_t i;
    for (i = 0; i < column->n_srcs; i++) {
      if (column->src_bufs[i]) {
        grn_obj_close(ctx, column->src_bufs[i]);
      }
    }
    GRNGO_FREE(column->db, column->src_bufs);
  }
  if (column->text_buf) {
    grn_obj_close(ctx, column->text_buf);
  }
  if (column->vector_buf) {
    grn_obj_close(ctx, column->vector_buf);
  }
  GRNGO_FREE(column->db, column);
}

static grn_rc
_grngo_open_src(grngo_db *db, grn_obj *table,
                const char *name, size_t name_len, grn_obj **src) {
  if ((name_len == GRN_COLUMN_NAME_KEY_LEN) &&
      !memcmp(name, GRN_COLUMN_NAME_KEY, name_len) &&
      (table->header.type == GRN_TABLE_NO_KEY)) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj *new_src;
  if ((name_len == GRN_COLUMN_NAME_VALUE_LEN) &&
      !memcmp(name, GRN_COLUMN_NAME_VALUE, name_len)) {
    new_src = grn_ctx_at(db->ctx, grn_obj_id(db->ctx, table));
  } else {
    new_src = grn_obj_column(db->ctx, table, name, name_len);
  }
  if (!new_src) {
    if (db->ctx->rc != GRN_SUCCESS) {
      return db->ctx->rc;
    }
    return GRN_UNKNOWN_ERROR;
  }
  *src = new_src;
  return GRN_SUCCESS;
}

static grn_rc
_grngo_push_src(grngo_column *column, grn_obj *table,
                const char *name, size_t name_len, grn_obj **next_table) {
  grn_obj *src;
  grn_rc rc = _grngo_open_src(column->db, table, name, name_len, &src);
  if (rc != GRN_SUCCESS) {
    return rc;
  }
  grn_ctx *ctx = column->db->ctx;
  switch (src->header.type) {
    case GRN_COLUMN_VAR_SIZE: {
      grn_obj_flags type = src->header.flags & GRN_OBJ_COLUMN_TYPE_MASK;
      if (type == GRN_OBJ_COLUMN_VECTOR) {
        column->dimension++;
      }
      // Fallthrough.
    }
    case GRN_TABLE_HASH_KEY: // _value.
    case GRN_TABLE_PAT_KEY:  // _value.
    case GRN_TABLE_NO_KEY:   // _value.
    case GRN_ACCESSOR:       // _id or _key.
    case GRN_COLUMN_FIX_SIZE: {
      grn_id range = grn_obj_get_range(ctx, src);
      if (range == GRN_DB_VOID) {
        grn_obj_unlink(ctx, src);
        return GRN_INVALID_ARGUMENT;
      } else if (range <= GRNGO_MAX_BUILTIN_TYPE_ID) {
        column->value_type = range;
        *next_table = NULL;
      } else {
        grn_obj *range_obj = grn_ctx_at(ctx, range);
        if (!grn_obj_is_table(ctx, range_obj)) {
          grn_obj_unlink(ctx, range_obj);
          return GRN_INVALID_ARGUMENT;
        }
        *next_table = range_obj;
      }
      break;
    }
    default: {
      grn_obj_unlink(ctx, src);
      return GRN_INVALID_ARGUMENT;
    }
  }
  // Append a source.
  size_t new_size = sizeof(grn_obj *) * (column->n_srcs + 1);
  grn_obj **new_srcs = (grn_obj **)GRNGO_REALLOC(column->db, column->srcs,
                                                 new_size);
  if (!new_srcs) {
    if (*next_table) {
      grn_obj_unlink(ctx, *next_table);
    }
    grn_obj_unlink(ctx, src);
    return GRN_NO_MEMORY_AVAILABLE;
  }
  column->srcs = new_srcs;
  column->srcs[column->n_srcs] = src;
  column->n_srcs++;
  return GRN_SUCCESS;
}

static grn_rc
_grngo_open_bufs(grngo_column *column) {
  size_t size = sizeof(grn_obj *) * column->n_srcs;
  column->src_bufs = (grn_obj **)GRNGO_MALLOC(column->db, size);
  if (!column->src_bufs) {
    return GRN_NO_MEMORY_AVAILABLE;
  }
  size_t i = 0;
  for (i = 0; i < column->n_srcs; i++) {
    column->src_bufs[i] = NULL;
  }
  grn_ctx *ctx = column->db->ctx;
  // Open buffers for table references.
  for (i = 0; i < (column->n_srcs - 1); i++) {
    column->src_bufs[i] = grn_obj_open(ctx, GRN_UVECTOR, 0, GRN_DB_UINT32);
    if (!column->src_bufs[i]) {
      if (ctx->rc != GRN_SUCCESS) {
        return ctx->rc;
      }
      return GRN_UNKNOWN_ERROR;
    }
  }
  // Open buffers for values.
  grn_builtin_type value_type = column->value_type;
  switch (value_type) {
    case GRN_DB_SHORT_TEXT:
    case GRN_DB_TEXT:
    case GRN_DB_LONG_TEXT: {
      if (_grngo_is_vector(column->srcs[i])) {
        column->src_bufs[i] = grn_obj_open(ctx, GRN_VECTOR, 0, value_type);
      } else {
        column->src_bufs[i] = grn_obj_open(ctx, GRN_BULK, 0, GRN_DB_LONG_TEXT);
      }
      if (!column->src_bufs[i]) {
        if (ctx->rc != GRN_SUCCESS) {
          return ctx->rc;
        }
        return GRN_UNKNOWN_ERROR;
      }
      column->text_buf = grn_obj_open(ctx, GRN_BULK, 0, GRN_DB_LONG_TEXT);
      if (!column->text_buf) {
        if (ctx->rc != GRN_SUCCESS) {
          return ctx->rc;
        }
        return GRN_UNKNOWN_ERROR;
      }
      break;
    }
    default: {
      column->src_bufs[i] = grn_obj_open(ctx, GRN_UVECTOR, 0, value_type);
      if (!column->src_bufs[i]) {
        if (ctx->rc != GRN_SUCCESS) {
          return ctx->rc;
        }
        return GRN_UNKNOWN_ERROR;
      }
      break;
    }
  }
  // Open a buffer for vectors.
  if (column->dimension != 0) {
    column->vector_buf = grn_obj_open(ctx, GRN_BULK, 0, GRN_DB_LONG_TEXT);
    if (!column->vector_buf) {
      if (ctx->rc != GRN_SUCCESS) {
        return ctx->rc;
      }
      return GRN_UNKNOWN_ERROR;
    }
  }
  return GRN_SUCCESS;
}

static grn_rc
_grngo_open_column(grngo_table *table, grngo_column *column,
                   const char *name, size_t name_len) {
  // Tokenize the given name and push sources.
  grn_obj *owner = table->objs[0];
  while (name_len) {
    if (!owner) {
      return GRN_INVALID_ARGUMENT;
    }
    const char *token = name;
    size_t token_len = 0;
    while (name_len) {
      name_len--;
      if (*name++ == '.') {
        break;
      }
      token_len++;
    }
    grn_obj *new_owner;
    grn_rc rc = _grngo_push_src(column, owner, token, token_len, &new_owner);
    if (rc != GRN_SUCCESS) {
      return rc;
    }
    if (column->n_srcs != 0) {
      grn_obj_unlink(column->db->ctx, owner);
    }
    owner = new_owner;
  }
  // Check whether the column is writable or not.
  if (column->n_srcs == 1) {
    switch (column->srcs[0]->header.type) {
      case GRN_TABLE_HASH_KEY: // _value.
      case GRN_TABLE_PAT_KEY:  // _value.
      case GRN_TABLE_NO_KEY:   // _value.
      case GRN_COLUMN_FIX_SIZE:
      case GRN_COLUMN_VAR_SIZE: {
        column->writable = GRN_TRUE;
        break;
      }
      default: {
        break;
      }
    }
  }
  // Resolve the _key chain if _key is table reference.
  while (owner) {
    grn_obj *new_owner;
    grn_rc rc = _grngo_push_src(column, owner, GRN_COLUMN_NAME_KEY,
                                GRN_COLUMN_NAME_KEY_LEN, &new_owner);
    if (rc != GRN_SUCCESS) {
      return rc;
    }
    grn_obj_unlink(column->db->ctx, owner);
    owner = new_owner;
  }
  return _grngo_open_bufs(column);
}

grn_rc
grngo_open_column(grngo_table *table, const char *name, size_t name_len,
                  grngo_column **column) {
  if (!table || !name || (name_len == 0) || !column) {
    return GRN_INVALID_ARGUMENT;
  }
  grngo_column *new_column = _grngo_new_column(table);
  grn_rc rc = new_column ? GRN_SUCCESS : GRN_NO_MEMORY_AVAILABLE;
  if (rc == GRN_SUCCESS) {
    rc = _grngo_open_column(table, new_column, name, name_len);
    if (rc == GRN_SUCCESS) {
      *column = new_column;
    } else {
      _grngo_delete_column(new_column);
    }
  }
  return rc;
}

void
grngo_close_column(grngo_column *column) {
  if (column) {
    _grngo_delete_column(column);
  }
}

grn_rc
grngo_set_bool(grngo_column *column, grn_id id, grn_bool value) {
  if (!column || !column->writable || !GRNGO_TEST_BOOL(value)) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_ctx *ctx = column->db->ctx;
  if (grn_table_at(ctx, column->table->objs[0], id) == GRN_ID_NIL) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj obj;
  GRN_BOOL_INIT(&obj, 0);
  grn_rc rc = grn_bulk_write(ctx, &obj, (const char *)&value, sizeof(value));
  if (rc == GRN_SUCCESS) {
    rc = grn_obj_set_value(ctx, column->srcs[0], id, &obj, GRN_OBJ_SET);
  }
  GRN_OBJ_FIN(ctx, &obj);
  return rc;
}

#define GRNGO_SET_INT_CASE_BLOCK(type)\
  case GRN_DB_ ## type: {\
    if (!GRNGO_TEST_ ## type(value)) {\
      return GRN_INVALID_ARGUMENT;\
    }\
    GRN_ ## type ## _INIT(&obj, 0);\
    GRNGO_DB_TYPE(type) db_value = (GRNGO_DB_TYPE(type))value;\
    rc = grn_bulk_write(ctx, &obj, (const char *)&db_value, sizeof(db_value));\
    break;\
  }
grn_rc
grngo_set_int(grngo_column *column, grn_id id, int64_t value) {
  if (!column || !column->writable) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_ctx *ctx = column->db->ctx;
  if (grn_table_at(ctx, column->table->objs[0], id) == GRN_ID_NIL) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj obj;
  grn_rc rc;
  switch (column->value_type) {
    GRNGO_SET_INT_CASE_BLOCK(INT8)
    GRNGO_SET_INT_CASE_BLOCK(INT16)
    GRNGO_SET_INT_CASE_BLOCK(INT32)
    GRNGO_SET_INT_CASE_BLOCK(INT64)
    GRNGO_SET_INT_CASE_BLOCK(UINT8)
    GRNGO_SET_INT_CASE_BLOCK(UINT16)
    GRNGO_SET_INT_CASE_BLOCK(UINT32)
    GRNGO_SET_INT_CASE_BLOCK(UINT64)
    GRNGO_SET_INT_CASE_BLOCK(TIME)
    default: {
      return GRN_INVALID_ARGUMENT;
    }
  }
  if (rc == GRN_SUCCESS) {
    rc = grn_obj_set_value(ctx, column->srcs[0], id, &obj, GRN_OBJ_SET);
  }
  GRN_OBJ_FIN(ctx, &obj);
  return rc;
}
#undef GRNGO_SET_INT_CASE_BLOCK

grn_rc
grngo_set_float(grngo_column *column, grn_id id, double value) {
  if (!column || !column->writable || !GRNGO_TEST_FLOAT(value)) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_ctx *ctx = column->db->ctx;
  if (grn_table_at(ctx, column->table->objs[0], id) == GRN_ID_NIL) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj obj;
  GRN_FLOAT_INIT(&obj, 0);
  grn_rc rc = grn_bulk_write(ctx, &obj, (const char *)&value, sizeof(value));
  if (rc == GRN_SUCCESS) {
    rc = grn_obj_set_value(ctx, column->srcs[0], id, &obj, GRN_OBJ_SET);
  }
  GRN_OBJ_FIN(ctx, &obj);
  return rc;
}

#define GRNGO_SET_TEXT_CASE_BLOCK(type)\
  case GRN_DB_ ## type: {\
    if (!GRNGO_TEST_ ## type(value)) {\
      return GRN_INVALID_ARGUMENT;\
    }\
    GRN_ ## type ## _INIT(&obj, 0);\
    break;\
  }
grn_rc
grngo_set_text(grngo_column *column, grn_id id, grngo_text value) {
  if (!column || !column->writable) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_ctx *ctx = column->db->ctx;
  if (grn_table_at(ctx, column->table->objs[0], id) == GRN_ID_NIL) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj obj;
  switch (column->value_type) {
    GRNGO_SET_TEXT_CASE_BLOCK(SHORT_TEXT)
    GRNGO_SET_TEXT_CASE_BLOCK(TEXT)
    GRNGO_SET_TEXT_CASE_BLOCK(LONG_TEXT)
    default: {
      return GRN_UNKNOWN_ERROR;
    }
  }
  grn_rc rc = grn_bulk_write(ctx, &obj, value.ptr, value.size);
  if (rc == GRN_SUCCESS) {
    rc = grn_obj_set_value(ctx, column->srcs[0], id, &obj, GRN_OBJ_SET);
  }
  GRN_OBJ_FIN(ctx, &obj);
  return rc;
}
#undef GRNGO_SET_TEXT_CASE_BLOCK

grn_rc
grngo_set_geo_point(grngo_column *column, grn_id id, grn_geo_point value) {
  if (!column || !column->writable || !GRNGO_TEST_GEO_POINT(value)) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_ctx *ctx = column->db->ctx;
  if (grn_table_at(ctx, column->table->objs[0], id) == GRN_ID_NIL) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj obj;
  switch (column->value_type) {
    case GRN_DB_TOKYO_GEO_POINT: {
      GRN_TOKYO_GEO_POINT_INIT(&obj, 0);
      break;
    }
    case GRN_DB_WGS84_GEO_POINT: {
      GRN_WGS84_GEO_POINT_INIT(&obj, 0);
      break;
    }
    default: {
      return GRN_UNKNOWN_ERROR;
    }
  }
  grn_rc rc = grn_bulk_write(ctx, &obj, (const char *)&value, sizeof(value));
  if (rc == GRN_SUCCESS) {
    rc = grn_obj_set_value(ctx, column->srcs[0], id, &obj, GRN_OBJ_SET);
  }
  GRN_OBJ_FIN(ctx, &obj);
  return rc;
}

grn_rc
grngo_set_bool_vector(grngo_column *column, grn_id id, grngo_vector value) {
  if (!column || !column->writable || !GRNGO_TEST_VECTOR(value)) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_ctx *ctx = column->db->ctx;
  if (grn_table_at(ctx, column->table->objs[0], id) == GRN_ID_NIL) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj obj;
  GRN_BOOL_INIT(&obj, GRN_OBJ_VECTOR);
  grn_rc rc = grn_bulk_write(ctx, &obj, (const char *)value.ptr,
                             sizeof(grn_bool) * value.size);
  if (rc == GRN_SUCCESS) {
    rc = grn_obj_set_value(ctx, column->srcs[0], id, &obj, GRN_OBJ_SET);
  }
  GRN_OBJ_FIN(ctx, &obj);
  return rc;
}

#define GRNGO_SET_INT_VECTOR_CASE_BLOCK(type)\
  case GRN_DB_ ## type: {\
    for (i = 0; i < value.size; i++) {\
      if (!GRNGO_TEST_ ## type(values[i])) {\
        return GRN_INVALID_ARGUMENT;\
      }\
    }\
    GRN_ ## type ## _INIT(&obj, GRN_OBJ_VECTOR);\
    rc = grn_bulk_space(ctx, &obj, sizeof(GRNGO_DB_TYPE(type)) * value.size);\
    if (rc != GRN_SUCCESS) {\
      break;\
    }\
    GRNGO_DB_TYPE(type) *head = (GRNGO_DB_TYPE(type) *)GRN_BULK_HEAD(&obj);\
    for (i = 0; i < value.size; i++) {\
      head[i] = (GRNGO_DB_TYPE(type))values[i];\
    }\
    break;\
  }
grn_rc
grngo_set_int_vector(grngo_column *column, grn_id id, grngo_vector value) {
  if (!column || !column->writable || !GRNGO_TEST_VECTOR(value)) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_ctx *ctx = column->db->ctx;
  if (grn_table_at(ctx, column->table->objs[0], id) == GRN_ID_NIL) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj obj;
  size_t i;
  const int64_t *values = (const int64_t *)value.ptr;
  grn_rc rc = GRN_SUCCESS;
  switch (column->value_type) {
    GRNGO_SET_INT_VECTOR_CASE_BLOCK(INT8)
    GRNGO_SET_INT_VECTOR_CASE_BLOCK(INT16)
    GRNGO_SET_INT_VECTOR_CASE_BLOCK(INT32)
    GRNGO_SET_INT_VECTOR_CASE_BLOCK(INT64)
    GRNGO_SET_INT_VECTOR_CASE_BLOCK(UINT8)
    GRNGO_SET_INT_VECTOR_CASE_BLOCK(UINT16)
    GRNGO_SET_INT_VECTOR_CASE_BLOCK(UINT32)
    GRNGO_SET_INT_VECTOR_CASE_BLOCK(UINT64)
    GRNGO_SET_INT_VECTOR_CASE_BLOCK(TIME)
    default: {
      return GRN_INVALID_ARGUMENT;
    }
  }
  if (rc == GRN_SUCCESS) {
    rc = grn_obj_set_value(ctx, column->srcs[0], id, &obj, GRN_OBJ_SET);
  }
  GRN_OBJ_FIN(ctx, &obj);
  return rc;
}
#undef GRNGO_SET_INT_VECTOR_CASE_BLOCK

grn_rc
grngo_set_float_vector(grngo_column *column, grn_id id, grngo_vector value) {
  if (!column || !column->writable || !GRNGO_TEST_VECTOR(value)) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_ctx *ctx = column->db->ctx;
  if (grn_table_at(ctx, column->table->objs[0], id) == GRN_ID_NIL) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj obj;
  GRN_FLOAT_INIT(&obj, GRN_OBJ_VECTOR);
  grn_rc rc = grn_bulk_write(ctx, &obj, (const char *)value.ptr,
                             sizeof(double) * value.size);
  if (rc == GRN_SUCCESS) {
    rc = grn_obj_set_value(ctx, column->srcs[0], id, &obj, GRN_OBJ_SET);
  }
  GRN_OBJ_FIN(ctx, &obj);
  return rc;
}

#define GRNGO_SET_TEXT_VECTOR_CASE_BLOCK(type)\
  case GRN_DB_ ## type: {\
    for (i = 0; i < value.size; i++) {\
      if (!GRNGO_TEST_ ## type(values[i])) {\
        return GRN_INVALID_ARGUMENT;\
      }\
    }\
    GRN_ ## type ## _INIT(&obj, GRN_OBJ_VECTOR);\
    break;\
  }
grn_rc
grngo_set_text_vector(grngo_column *column, grn_id id, grngo_vector value) {
  if (!column || !column->writable || !GRNGO_TEST_VECTOR(value)) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_ctx *ctx = column->db->ctx;
  if (grn_table_at(ctx, column->table->objs[0], id) == GRN_ID_NIL) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj obj;
  size_t i;
  const grngo_text *values = (const grngo_text *)value.ptr;
  switch (column->value_type) {
    GRNGO_SET_TEXT_VECTOR_CASE_BLOCK(SHORT_TEXT)
    GRNGO_SET_TEXT_VECTOR_CASE_BLOCK(TEXT)
    GRNGO_SET_TEXT_VECTOR_CASE_BLOCK(LONG_TEXT)
    default: {
      return GRN_UNKNOWN_ERROR;
    }
  }
  grn_rc rc = GRN_SUCCESS;
  for (i = 0; i < value.size; i++) {
    rc = grn_vector_add_element(ctx, &obj, (const char *)values[i].ptr,
                                values[i].size, 0, obj.header.domain);
    if (rc != GRN_SUCCESS) {
      break;
    }
  }
  if (rc == GRN_SUCCESS) {
    rc = grn_obj_set_value(ctx, column->srcs[0], id, &obj, GRN_OBJ_SET);
  }
  GRN_OBJ_FIN(ctx, &obj);
  return rc;
}
#undef GRNGO_SET_TEXT_VECTOR_CASE_BLOCK

grn_rc
grngo_set_geo_point_vector(grngo_column *column, grn_id id,
                           grngo_vector value) {
  if (!column || !column->writable || !GRNGO_TEST_VECTOR(value)) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_ctx *ctx = column->db->ctx;
  if (grn_table_at(ctx, column->table->objs[0], id) == GRN_ID_NIL) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_obj obj;
  switch (column->value_type) {
    case GRN_DB_TOKYO_GEO_POINT: {
      GRN_TOKYO_GEO_POINT_INIT(&obj, GRN_OBJ_VECTOR);
      break;
    }
    case GRN_DB_WGS84_GEO_POINT: {
      GRN_WGS84_GEO_POINT_INIT(&obj, GRN_OBJ_VECTOR);
      break;
    }
    default: {
      return GRN_UNKNOWN_ERROR;
    }
  }
  grn_rc rc = grn_bulk_write(ctx, &obj, (const char *)value.ptr,
                             sizeof(grn_geo_point) * value.size);
  if (rc == GRN_SUCCESS) {
    rc = grn_obj_set_value(ctx, column->srcs[0], id, &obj, GRN_OBJ_SET);
  }
  GRN_OBJ_FIN(ctx, &obj);
  return rc;
}

static grn_rc
_grngo_get_ref(grngo_column *column, size_t src_id,
               const grn_id *ids, size_t n_ids,
               const grn_id **next_ids, size_t *next_n_ids) {
  grn_ctx *ctx = column->db->ctx;
  grn_obj *src = column->srcs[src_id];
  grn_obj *src_buf = column->src_bufs[src_id];
  GRN_BULK_REWIND(src_buf);
  if (_grngo_is_vector(src)) {
    size_t i;
    size_t offset = 0;
    for (i = 0; i < n_ids; i++) {
      grn_obj_get_value(ctx, src, ids[i], src_buf);
      if (ctx->rc != GRN_SUCCESS) {
        return ctx->rc;
      }
      size_t size = grn_vector_size(ctx, src_buf);
      grngo_vector vector = { NULL, size - offset };
      grn_rc rc = grn_bulk_write(ctx, column->vector_buf,
                                 (char *)&vector, sizeof(vector));
      if (rc != GRN_SUCCESS) {
        return rc;
      }
      offset = size;
    }
  } else {
    size_t i;
    for (i = 0; i < n_ids; i++) {
      grn_obj_get_value(ctx, src, ids[i], src_buf);
      if (ctx->rc != GRN_SUCCESS) {
        return ctx->rc;
      }
    }
  }
  *next_ids = (const grn_id *)GRN_BULK_HEAD(src_buf);
  *next_n_ids = grn_vector_size(ctx, src_buf);
  return GRN_SUCCESS;
}

static grn_rc
_grngo_get_value(grngo_column *column, const grn_id *ids, size_t n_ids) {
  grn_ctx *ctx = column->db->ctx;
  size_t src_id = column->n_srcs - 1;
  grn_obj *src = column->srcs[src_id];
  grn_obj *src_buf = column->src_bufs[src_id];
  GRN_BULK_REWIND(src_buf);
  if (_grngo_is_vector(src)) {
    size_t i;
    size_t offset = 0;
    for (i = 0; i < n_ids; i++) {
      grn_obj_get_value(ctx, src, ids[i], src_buf);
      if (ctx->rc != GRN_SUCCESS) {
        return ctx->rc;
      }
      size_t size = grn_vector_size(ctx, src_buf);
      grngo_vector vector = { NULL, size - offset };
      grn_rc rc = grn_bulk_write(ctx, column->vector_buf,
                                 (char *)&vector, sizeof(vector));
      if (rc != GRN_SUCCESS) {
        return rc;
      }
      offset = size;
    }
    if (column->text_buf) {
      GRN_BULK_REWIND(column->text_buf);
      for (i = 0; i < offset; i++) {
        grngo_text text;
        text.size = grn_vector_get_element(ctx, src_buf, i,
                                           &text.ptr, NULL, NULL);
        grn_rc rc = grn_bulk_write(ctx, column->text_buf,
                                   (char *)&text, sizeof(text));
        if (rc != GRN_SUCCESS) {
          return rc;
        }
      }
    }
  } else if (column->text_buf) {
    GRN_BULK_REWIND(column->text_buf);
    size_t i;
    size_t offset = 0;
    for (i = 0; i < n_ids; i++) {
      grn_obj_get_value(ctx, src, ids[i], src_buf);
      if (ctx->rc != GRN_SUCCESS) {
        return ctx->rc;
      }
      size_t size = GRN_BULK_VSIZE(src_buf);
      grngo_text text = { NULL, size - offset };
      grn_rc rc = grn_bulk_write(ctx, column->text_buf,
                                 (char *)&text, sizeof(text));
      if (rc != GRN_SUCCESS) {
        return rc;
      }
      offset = size;
    }
    char *ptr = GRN_BULK_HEAD(src_buf);
    grngo_text *texts = (grngo_text *)GRN_BULK_HEAD(column->text_buf);
    for (i = 0; i < n_ids; i++) {
      texts[i].ptr = ptr;
      ptr += texts[i].size;
    }
  } else {
    size_t i;
    for (i = 0; i < n_ids; i++) {
      grn_obj_get_value(ctx, src, ids[i], src_buf);
      if (ctx->rc != GRN_SUCCESS) {
        return ctx->rc;
      }
    }
  }
  return GRN_SUCCESS;
}

#define GRNGO_FILL_VECTOR_CASE_BLOCK(type)\
  case GRN_DB_ ## type: {\
    void *head = GRN_BULK_HEAD(column->src_bufs[column->n_srcs - 1]);\
    GRNGO_DB_TYPE(type) *ptr = (GRNGO_DB_TYPE(type) *)head;\
    while (src < dest) {\
      src->ptr = ptr;\
      ptr += src->size;\
      src++;\
    }\
    break;\
  }
static grn_rc
_grngo_fill_vector(grngo_column *column) {
  // Fill pointers to next vectors.
  grngo_vector *src = (grngo_vector *)GRN_BULK_HEAD(column->vector_buf);
  grngo_vector *dest = src + 1;
  size_t i, j;
  for (i = 1; i < column->dimension; i++) {
    size_t size = dest - src;
    for (j = 0; j < size; j++) {
      src->ptr = dest;
      dest += src->size;
      src++;
    }
  }
  // Fill pointers to text bodies.
  if (column->text_buf) {
    grngo_text *ptr = (grngo_text *)GRN_BULK_HEAD(column->text_buf);
    while (src < dest) {
      src->ptr = ptr;
      ptr += src->size;
      src++;
    }
    return GRN_SUCCESS;
  }
  // Fill pointers to values.
  switch (column->value_type) {
    GRNGO_FILL_VECTOR_CASE_BLOCK(BOOL)
    GRNGO_FILL_VECTOR_CASE_BLOCK(INT8)
    GRNGO_FILL_VECTOR_CASE_BLOCK(INT16)
    GRNGO_FILL_VECTOR_CASE_BLOCK(INT32)
    GRNGO_FILL_VECTOR_CASE_BLOCK(INT64)
    GRNGO_FILL_VECTOR_CASE_BLOCK(UINT8)
    GRNGO_FILL_VECTOR_CASE_BLOCK(UINT16)
    GRNGO_FILL_VECTOR_CASE_BLOCK(UINT32)
    GRNGO_FILL_VECTOR_CASE_BLOCK(UINT64)
    GRNGO_FILL_VECTOR_CASE_BLOCK(FLOAT)
    GRNGO_FILL_VECTOR_CASE_BLOCK(TIME)
    GRNGO_FILL_VECTOR_CASE_BLOCK(TOKYO_GEO_POINT)
    GRNGO_FILL_VECTOR_CASE_BLOCK(WGS84_GEO_POINT)
    default: {
      return GRN_UNKNOWN_ERROR;
    }
  }
  return GRN_SUCCESS;
}
#undef GRNGO_FILL_VECTOR_CASE_BLOCK

grn_rc
grngo_get(grngo_column *column, grn_id id, void **value) {
  if (!column || !value) {
    return GRN_INVALID_ARGUMENT;
  }
  grn_ctx *ctx = column->db->ctx;
  if (grn_table_at(ctx, column->table->objs[0], id) == GRN_ID_NIL) {
    return GRN_INVALID_ARGUMENT;
  }
  // Get vectors and values.
  if (column->vector_buf) {
    GRN_BULK_REWIND(column->vector_buf);
  }
  const grn_id *ids = &id;
  size_t n_ids = 1;
  size_t i;
  for (i = 0; i < (column->n_srcs - 1); i++) {
    grn_rc rc = _grngo_get_ref(column, i, ids, n_ids, &ids, &n_ids);
    if (rc != GRN_SUCCESS) {
      return rc;
    }
  }
  grn_rc rc = _grngo_get_value(column, ids, n_ids);
  if (rc != GRN_SUCCESS) {
    return GRN_SUCCESS;
  }
  // Fill pointers.
  if (column->dimension != 0) {
    _grngo_fill_vector(column);
    *value = GRN_BULK_HEAD(column->vector_buf);
  } else if (column->text_buf) {
    *value = GRN_BULK_HEAD(column->text_buf);
  } else {
    *value = GRN_BULK_HEAD(column->src_bufs[i]);
  }
  return GRN_SUCCESS;
}
