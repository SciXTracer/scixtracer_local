"""Implementation of SQLite queries"""
import os
from sqlite3 import Connection, OperationalError

import pandas as pd
from scixtracer.logger import logger


def __fetchall(conn: Connection,
               sql: str,
               parameters: list[float | int | bool | str | None] = None):
    """Run a sql query

    :param conn: Connection to the database
    :param sql: query in sqlite
    :param parameters: query arguments
    :return the query result
    """
    cur = conn.cursor()
    if parameters is not None:
        cur.execute(sql, parameters)
    else:
        cur.execute(sql)
    return cur.fetchall()


def __fetchone(conn: Connection,
               sql: str,
               parameters: list[float | int | bool | str | None] = None):
    """Exec a query and fetch one

    :param conn: Connection to the database
    :param sql: SQL query
    :param parameters: list of query parameters
    """
    cur = conn.cursor()
    if parameters is not None:
        cur.execute(sql, parameters)
    else:
        cur.execute(sql)
    return cur.fetchone()


def init_database(conn: Connection):
    """Initialize the database

    :param conn: Connection to the database
    """
    root = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(root, 'schema.sql')
    with open(filename, 'r', encoding='utf-8') as fd_:
        sql_file = fd_.read()

    cur = conn.cursor()
    sql_commands = sql_file.split(';')
    for command in sql_commands:
        try:
            cur.execute(command)
            if 'INSERT' in command:
                conn.commit()
        except OperationalError as err:
            logger().error(f"Sqlite operational error: {str(err)}")
            raise ValueError from err


def insert_location(conn: Connection) -> int:
    """Add a new location to the database

    :param conn: Connection to the database
    :return: the ID of the new location
    """
    sql = '''INSERT INTO location DEFAULT VALUES'''
    curr = conn.cursor()
    curr.execute(sql)
    conn.commit()
    return int(curr.lastrowid)


def insert_annotation_key(conn: Connection, key: str) -> int:
    """Insert an annotation if not exists, otherwise get its ID

    :param conn: Connection to the database
    :param key: Name of the annotation
    :return: the ID of the new or already existing annotation
    """
    sql = """SELECT id FROM annotation_key WHERE name=?"""
    ann_id = __fetchone(conn, sql, [key])
    if ann_id is None:
        sql = """INSERT INTO annotation_key (name) VALUES (?)"""
        cur = conn.cursor()
        cur.execute(sql, [key])
        conn.commit()
        ann_id = cur.lastrowid
    else:
        ann_id = ann_id[0]
    return ann_id


def insert_location_annotation(conn: Connection,
                               location_id: str,
                               key: str,
                               value: str):
    """Insert a location annotation to the DB

    :param conn: Connection to the database
    :param location_id: ID of the location to annotate,
    :param key: Annotation key,
    :param value: Annotation value
    """
    key_id = insert_annotation_key(conn, key)
    sql = """INSERT INTO location_annotation (location_id, key_id, value)
             VALUES (?, ?, ?)
          """
    conn.cursor().execute(sql, [location_id, key_id, value])
    conn.commit()


def insert_data_annotation(conn: Connection,
                           data_uri: str,
                           key: str,
                           value: str):
    """Insert a data annotation to the DB

    :param conn: Connection to the database,
    :param data_uri: URI of the data to annotate,
    :param key: Annotation key,
    :param value: Annotation value
    """
    key_id = insert_annotation_key(conn, key)
    sql = """INSERT INTO data_annotation (data_id, key_id, value)
             VALUES ((SELECT id FROM data WHERE uri=?), ?, ?)
          """
    conn.cursor().execute(sql, [data_uri, key_id, value])
    conn.commit()


def storage_type_id(conn: Connection, storage_type: str) -> int:
    """Get the ID of the storage format

    :param conn: Connection to the database
    :param storage_type: Storage format,
    :return: The storage type ID
    """
    sql = "SELECT id FROM storage_type WHERE name=?"
    id_ = __fetchone(conn, sql, [storage_type])
    if id_ is not None:
        return id_[0]
    raise ValueError("storage_type_id: storage type not found")


def insert_data(conn: Connection,
                location_id: int,
                uri: str,
                storage_type: str,
                metadata_uri: str):
    """Insert a new data entry

    :param conn: Connection to the database,
    :param location_id: UUID of the location,
    :param uri: Annotation key,
    :param storage_type: Storage format,
    :param metadata_uri: The URI of the metadata
    """
    storage_id = storage_type_id(conn, storage_type)
    if storage_id is None:
        raise ValueError(f'Storage type {storage_type} not recognized')
    sql = """INSERT INTO data (location_id, type_id, uri, metadata_uri)
             VALUES (?, ?, ?, ?)
          """
    cur = conn.cursor()
    cur.execute(sql, [location_id, storage_id, uri, metadata_uri])
    conn.commit()
    return cur.lastrowid


def query_data_with_locations(conn: Connection,
                              location_ids: list[int]):
    """Query data at a given locations

    DEPRECATED: Should be removed?

    :param conn: Connection to the database,
    :param location_ids: UUIDs of the location,
    """
    location_str = " ,".join(str(x) for x in location_ids)[:-1]
    sql = f"""SELECT uri, location_id, st.name
              FROM data
              INNER JOIN storage_type AS st ON st.id == data.type_id
              WHERE location_id IN ({location_str})"""
    return __fetchall(conn, sql)


def __query_data_with_annotations_both(conn: Connection,
                                       annotations: dict[str, any]):
    conditions = query_annotations_conditions(annotations)

    sql = f"""WITH location_count AS (
                 SELECT location_id, COUNT(1) as loc_num
                 FROM location_annotation
                 WHERE ({conditions})
                 GROUP BY location_id
             ),
             data_count AS (
                 SELECT data_annotation.data_id, 
                        data.location_id,
                        COUNT(1) as data_num
                 FROM data_annotation
                 INNER JOIN data ON data.id = data_annotation.data_id
                 WHERE ({conditions})
                 GROUP BY data_annotation.data_id
             )
             SELECT location_id, uri, storage_type.name as type, metadata_uri
             FROM data 
             INNER JOIN storage_type ON storage_type.id = data.type_id
             WHERE data.id IN (
                 SELECT data_id 
                 FROM data_count
                 INNER JOIN location_count 
                     ON location_count.location_id = data_count.location_id
                 WHERE loc_num+data_num={len(annotations)}
             )
    """
    result = __fetchall(conn, sql)
    return result


def __query_data_with_annotations_data(conn: Connection,
                                       annotations: dict[str, any]):
    conditions = query_annotations_conditions(annotations)
    sql = f"""WITH data_count AS (
                 SELECT data_id, 
                        COUNT(1) as data_num
                 FROM data_annotation
                 WHERE ({conditions})
                 GROUP BY data_id
             )
             SELECT location_id, 
                    uri, 
                    storage_type.name as type, 
                    metadata_uri
                 FROM data 
                 INNER JOIN storage_type ON storage_type.id = data.type_id
                 WHERE data.id IN (
                     SELECT data_id 
                     FROM data_count
                     WHERE data_num={len(annotations)}
             )
            """
    return __fetchall(conn, sql)


def __query_data_with_annotations_loc(conn: Connection,
                                      annotations: dict[str, any]):
    conditions = query_annotations_conditions(annotations)
    sql = f"""WITH location_count AS (
                 SELECT location_id, COUNT(1) as loc_num
                 FROM location_annotation
                 WHERE ({conditions})
                 GROUP BY location_id
             )
             SELECT location_id, uri, storage_type.name as type, metadata_uri
                 FROM data 
                 INNER JOIN storage_type ON storage_type.id = data.type_id
                 WHERE data.location_id IN (
                     SELECT location_id 
                     FROM location_count
                     WHERE loc_num={len(annotations)}
             )
            """
    return __fetchall(conn, sql)

def query_data_from_uri(conn: Connection, data_uri: str) -> list:
    """Read the data information from it URI

    :param conn: Connection to database,
    :param data_uri: URI of the data,
    :return: The information of the data
    """
    sql = f"""SELECT data.location_id, storage_type.name, data.uri, data.metadata_uri
              FROM data
              INNER JOIN storage_type ON storage_type.id = data.type_id
              WHERE data.uri = '{data_uri}'
           """
    return __fetchone(conn, sql)

def query_data_at(conn: Connection, locations: list[int]):
    """Implementation of the data at query

    :param conn: Connection to the database,
    :param locations: Locations to query,
    """
    locations_str = ",".join([f"'{value}'" for value in locations])
    sql = f"""SELECT data.location_id, data.uri, storage_type.name, data.metadata_uri
              FROM data
              INNER JOIN storage_type ON storage_type.id = data.type_id
              WHERE data.location_id in ({locations_str})
           """
    return __fetchall(conn, sql)

def query_data_with_annotations(conn: Connection,
                                annotations: dict[str, any]):
    """Query the id of data with given annotations

    :param conn: Connection to the database,
    :param annotations: Annotations of data,
    """
    anns = ",".join([f"'{value}'" for value in annotations.keys()])
    sql = f"""SELECT la.id
              FROM location_annotation AS la
              INNER JOIN annotation_key AS ak ON ak.id = la.key_id
              WHERE ak.name IN ({anns})
          """
    loc_ann = __fetchall(conn, sql)
    sql = f"""SELECT la.id
              FROM data_annotation AS la
              INNER JOIN annotation_key AS ak ON ak.id = la.key_id
              WHERE ak.name IN ({anns})
          """
    data_ann = __fetchall(conn, sql)

    if len(loc_ann) > 0 and len(data_ann) > 0:
        return __query_data_with_annotations_both(conn, annotations)
    if len(loc_ann) > 0 and len(data_ann) == 0:
        return __query_data_with_annotations_loc(conn, annotations)
    if len(loc_ann) == 0 and len(data_ann) > 0:
        return __query_data_with_annotations_data(conn, annotations)
    raise ValueError("query_data_with_annotations: No annotations to query")


def query_annotations_conditions(annotations: dict[str, any]) -> str:
    """SQL query to retrieve annotations

    :param annotations: Annotations to query
    :return: the SQL query text
    """
    sql_conditions = ""
    for key, value in annotations.items():
        sql_conditions += f"""(key_id = (
                                  SELECT id FROM annotation_key 
                                  WHERE name='{key}'
                                  ) AND value='{value}'
                               )
                           """
        sql_conditions += " OR "
    sql_conditions = sql_conditions[:-4]
    return sql_conditions


def query_location(conn: Connection,
                   annotations: dict[str, any] = None
                   ) -> list[tuple[int]]:
    """Query locations that have given annotations

    :param conn: Connection to the database,
    :param annotations: Annotations of locations,
    :return: list of location ids
    """
    if annotations is None or len(annotations) == 0:
        sql = """SELECT id FROM location"""
        return __fetchall(conn, sql)

    conditions = query_annotations_conditions(annotations)

    sql = f"""WITH location_count AS (
                SELECT location_id, COUNT(1) as num
                FROM location_annotation
                WHERE ({conditions})
                GROUP BY location_id
              )
              SELECT location_id FROM location_count 
              WHERE num={len(annotations)}
          """
    response = __fetchall(conn, sql)
    return response


def query_data_annotation(conn: Connection) -> dict[str, list[any]]:
    """Query all the annotations used for the data annotation

     :param conn: Connection to the database,
     :return: the list of all values for each annotation key
     """
    sql = """SELECT ak.name, GROUP_CONCAT(DISTINCT da.value ||'')
             FROM data_annotation AS da
             INNER JOIN annotation_key AS ak ON ak.id = da.key_id
             GROUP BY da.key_id"""
    values = __fetchall(conn, sql)
    out = {}
    for data in values:
        out[data[0]] = data[1].split(',')
    return out


def query_data_tuples(conn: Connection, annotations: list[dict[str: any]]):
    """Query tuples of data using annotations

    :param conn: Connection to the database,
    :param annotations: List of annotations to query
    """
    out_data = []
    for ann in annotations:
        out_data.append(pd.DataFrame(query_data_with_annotations(conn, ann),
                                     columns=["location_id", "uri", "type",
                                              "metadata_uri"]))
    dfo = out_data[0]
    for i, df_ in enumerate(out_data):
        if i > 0:
            dfo = dfo.merge(df_, left_on='location_id', right_on='location_id',
                            suffixes=('', f'_{i}'))
    return dfo


def query_locations_annotation(conn: Connection) -> dict[str, list[any]]:
    """Query the location annotations keys and values

    :param conn: Connection to the database,
    :return: dict containing all values for each annotation key
    """
    sql = """SELECT ak.name, GROUP_CONCAT(DISTINCT la.value ||'')
             FROM location_annotation AS la
             INNER JOIN annotation_key AS ak ON ak.id = la.key_id
             GROUP BY la.key_id"""
    values = __fetchall(conn, sql)
    out = {}
    for data in values:
        out[data[0]] = data[1].split(',')
    return out


def query_view_locations(conn: Connection) -> pd.DataFrame:
    """Query to generate a view of all available locations in the dataset

     :param conn: Connection to the database,
     :return: a dataframe to visualize the locations
     """
    sql = """SELECT DISTINCT la.key_id, ak.name
             FROM location_annotation as la
             INNER JOIN annotation_key AS ak ON ak.id = la.key_id 
          """
    keys = __fetchall(conn, sql)
    col_names = []
    series = []
    for key in keys:
        sql = f"""SELECT location_id, value
                  FROM location_annotation
                  WHERE key_id={key[0]}
               """
        values = __fetchall(conn, sql)
        col_names.append(key[1])
        idx, values = zip(*values)
        series.append(pd.Series(values, idx))
    df_ = pd.concat(series, axis=1)
    df_.columns = col_names
    df_['location_id'] = df_.index
    return df_


def __select_data_and_type(conn, loc_filter) -> pd.DataFrame:
    sql = f"""SELECT data.id, data.location_id, st.name
                  FROM data
                  INNER JOIN storage_type AS st ON st.id = data.type_id
                  {loc_filter}
               """
    results = __fetchall(conn, sql)
    return pd.DataFrame(results, columns=["data_id", "location", "format"])


def __select_loc_annotations(conn, loc_filter) -> pd.DataFrame:
    sql = """SELECT DISTINCT la.key_id, ak.name
                FROM location_annotation AS la
                INNER JOIN annotation_key AS ak ON ak.id = la.key_id
          """
    ann_keys = __fetchall(conn, sql)
    col_names = []
    series = []
    for key in ann_keys:
        sql = f"""SELECT location_id, value
                  FROM location_annotation
                  WHERE key_id={key[0]} {loc_filter}
               """
        values = __fetchall(conn, sql)
        col_names.append(key[1])
        idx, values = zip(*values)
        series.append(pd.Series(values, idx))
    df_loc_ann = pd.concat(series, axis=1)
    df_loc_ann.columns = col_names
    return df_loc_ann.rename_axis('location').reset_index()


def __select_data_annotations(conn, loc_filter) -> pd.DataFrame:
    sql = """SELECT DISTINCT la.key_id, ak.name
                 FROM data_annotation AS la
                 INNER JOIN annotation_key AS ak ON ak.id = la.key_id
          """
    ann_keys = __fetchall(conn, sql)
    col_names = []
    series = []
    for key in ann_keys:
        sql = f"""SELECT data_id, value
                      FROM data_annotation
                      WHERE key_id={key[0]} {loc_filter}
                   """
        values = __fetchall(conn, sql)
        col_names.append(key[1])
        idx, values = zip(*values)
        series.append(pd.Series(values, idx))
    df_data_ann = pd.concat(series, axis=1)
    df_data_ann.columns = col_names
    return df_data_ann.rename_axis('data_id').reset_index()


def query_view_data(conn: Connection,
                    locations: list[int]
                    ) -> pd.DataFrame:
    """Query a visualization table of all the data at given locations

    :param conn: Connection to the database,
    :param locations: Locations to filter (empty for all locations),
    :return: DataTable with the view
    """
    # location filters
    loc_filter = ""
    loc_filter2 = ""
    if len(locations) > 0:
        loc_ids = ",".join([str(elem) for elem in locations])
        loc_filter = f"WHERE location_id IN {loc_ids}"
        loc_filter2 = f"AND location_id IN {loc_ids}"

    # select data and types
    df_ = __select_data_and_type(conn, loc_filter)

    # select location annotations
    df_loc_ann = __select_loc_annotations(conn, loc_filter2)

    # select data annotations
    df_data_ann = __select_data_annotations(conn, loc_filter2)

    df_out = df_.merge(df_loc_ann)
    df_out = df_out.merge(df_data_ann)
    return df_out


def query_delete(conn: Connection, uri: str):
    """Delete a data entry

    :param conn: Connection to the database,
    :param uri: The URI of the data to delete
    """
    cur = conn.cursor()
    sql = """DELETE FROM data_annotation
             WHERE data_id = (SELECT id FROM data WHERE uri=?)"""
    cur.execute(sql, [uri])
    sql = """DELETE FROM data WHERE uri=?"""
    cur.execute(sql, [uri])
    conn.commit()
