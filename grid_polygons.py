import argparse
import shapely.geometry


def frange(start, stop, step=None):
    """A float-capable 'range' replacement"""
    step = step or 1.0
    assert start <= stop
    cur = start
    while cur < stop:
        yield min(cur, stop)
        cur += step

def box_coords(top, bottom, left, right, increment):
    """
    Given a top/bottom/left/right & increment, yields a PostgreSQL expression to create box(s) covering that area
    """
    for x in frange(left, right, increment):
        next_x = x + increment
        if next_x > right:
            break
        for y in frange(bottom, top, increment):
            next_y = y + increment
            if next_y > top:
                break

            #box = shapely.geometry.box(minx=x, miny=y, maxx=next_x, maxy=next_y)
            #yield box.wkb.encode("hex")
            yield "ST_SetSRID( ST_MakeBox2D(ST_Point({x}, {y}), ST_Point({next_x}, {next_y})), 4326)".format(x=x, y=y, next_x=next_x, next_y=next_y)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--increment', default=1.0, type=float)
    parser.add_argument('-t', '--top', default=90, type=float)
    parser.add_argument('-l', '--left', default=-180, type=float)
    parser.add_argument('-b', '--bottom', default=-90, type=float)
    parser.add_argument('-r', '--right', default=180, type=float)
    parser.add_argument('--input', type=str)
    parser.add_argument('--output', type=str)
    parser.add_argument('--include-truncate', default=False, action='store_true')

    parser.add_argument("--aggregate", default="collect", choices="collect, union")

    args = parser.parse_args()

    args.output_table = args.output.split(".")[0]
    args.output_column = args.output.split(".")[1]
    args.input_table = args.input.split(".")[0]
    args.input_column = args.input.split(".")[1]

    print "BEGIN;"
    if args.include_truncate:
        print "TRUNCATE TABLE {output_table};".format(output_table=args.output_table)
        

    print "CREATE TEMPORARY TABLE boxes ( id serial );"
    print "SELECT AddGeometryColumn('boxes', 'box', 4326, 'POLYGON', 2);"
    print "create index boxes__box on boxes USING GIST (box);"
    print "create unique index boxes__id on boxes (id);"

    for geom in box_coords(top=args.top, bottom=args.bottom, left=args.left, right=args.right, increment=args.increment):
        query = "INSERT INTO boxes (box) VALUES ({geom});".format(geom=geom)
        #query = "INSERT INTO {output_table} ({output_column}) VALUES ({geom});".format(output_table=args.output_table, output_column=args.output_column, geom=geom)
        print query

    print "CREATE TEMPORARY TABLE ungrouped_output ( id serial, boxid integer );"
    print "SELECT AddGeometryColumn('ungrouped_output', 'geom', 4326, 'MULTIPOLYGON', 2);"

    query ="""
        INSERT INTO ungrouped_output (boxid, geom)
            SELECT
                boxes.id AS boxid,
                CASE
                    WHEN ST_Within(boxes.box, {input_table}.{input_column}) THEN ST_Multi(boxes.box)
                    WHEN ST_Within({input_table}.{input_column}, boxes.box) THEN ST_Multi({input_table}.{input_column})
                    WHEN ST_Intersects({input_table}.{input_column}, boxes.box) THEN ST_CollectionExtract(ST_Multi(ST_Intersection({input_table}.{input_column}, boxes.box)), 3)
                    ELSE NULL
                END AS geom
                FROM
                    {input_table} JOIN boxes ON ({input_table}.{input_column} && boxes.box) ;
    """.format(input_table=args.input_table, input_column=args.input_column)
    query = query.replace("\n", "")
    print query

    print "DROP TABLE boxes;"

    if args.aggregate == 'collect':
        print "INSERT into {output_table} ({output_column}) SELECT ST_Multi(ST_CollectionExtract(ST_Collect(ungrouped_output.geom), 3)) as {output_column} FROM ungrouped_output GROUP BY boxid;".format(output_table=args.output_table, output_column=args.output_column)
    elif args.aggregate == 'union':
        if args.include_truncate:
            print "INSERT into {output_table} ({output_column}) SELECT ST_Multi(ST_CollectionExtract(ST_Collect(ungrouped_output.geom), 3)) as {output_column} FROM ungrouped_output GROUP BY boxid;".format(output_table=args.output_table, output_column=args.output_column)
            print "UPDATE {output_table} SET {output_column} = ST_Multi(ST_UnaryUnion({output_column}));".format(output_table=args.output_table, output_column=args.output_column)
        else:
            print "INSERT into {output_table} ({output_column}) SELECT ST_Multi(ST_CollectionExtract(ST_Union(ungrouped_output.geom), 3)) as {output_column} FROM ungrouped_output GROUP BY boxid;".format(output_table=args.output_table, output_column=args.output_column)
    else:
        raise NotImplementedError

    print "DROP TABLE ungrouped_output;"


    if False:

        # create a table of bboxes covering the whole area


        # insert into land_test (geom) select st_collectionextract(st_multi(st_union(case when st_within(test.box, land_polygons.the_geom) then st_multi(test.box) when st_within(land_polygons.the_geom, test.box) then st_multi(land_polygons.the_geom) when st_intersects(land_polygons.the_geom, test.box) then st_multi(st_intersection(land_polygons.the_geom, test.box)) else NULL end)), 3) as geom from land_polygons join test on (land_polygons.the_geom && test.box ) group by test.id;

        # Now grid the other table with 
        #select st_collectionextract(st_collect(case when st_within(land_polygons.the_geom, test.box) then st_multi(land_polygons.the_geom) else st_multi(st_intersection(land_polygons.the_geom, test.box)) end), 3) as geom from land_polygons join test on (land_polygons.the_geom && test.box AND st_intersects(land_polygons.the_geom, test.box)) group by test.id
        old_query = """INSERT INTO {output_table} ({output_column})
                    SELECT {output_column} FROM (
                        SELECT ST_Multi(ST_Union(
                            CASE
                                WHEN ST_Within({input_column}, {geom}) THEN {input_column}
                                ELSE ST_Intersection({input_column}, {geom})
                            END
                        )) as {output_column}
                        FROM {input_table} WHERE {input_column} && {geom}
                    ) AS inner_table WHERE NOT ST_IsEmpty({output_column});
                    """.format(output_table=args.output_table, input_table=args.input_table, geom=geom, input_column=args.input_column, output_column=args.output_column)

        query = query.replace("\n", "")
        print query
        
    print "COMMIT;"

if __name__ == '__main__':
    main()
