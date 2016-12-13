import os
from sqlalchemy import create_engine
from ci.db.pgdbhelper import PGDbHelper
from ci.db.sqa.mapper import Mapper
from ci.config import get_instance


__author__ = 'ujjwal'


config = get_instance()
pgdb_helper = PGDbHelper(conn_str=config.pgsql_conn_str(), echo=True)

engine = create_engine(config.sqa_connection_string())
mapper = Mapper(engine=engine)
mapper.map_tables()

missing_data = -999


def generate_output_file(fname):
    if os.path.exists(fname):
        os.remove(fname)

    with open(fname, 'w') as of:
        sql = """
            select distinct(var.name)
            from forecast_roi_stats rs
            left join variable var on var.id=rs.variable_id
            order by var.name asc
            """
        rows = pgdb_helper.query(sql)
        var_names = []
        header_row = ["roi_id", "starttime", "endtime", "type", "iarea", "lat", "lon"]
        for row in rows:
            var_names.append(row[0])
            header_row = header_row + ["%s_count" % str(row[0]), "%s_sum" % str(row[0]), "%s_mean" % str(row[0]),
                                       "%s_stddev" % str(row[0]), "%s_min" % str(row[0]), "%s_max" % str(row[0])]

        of.write(",".join([str(h) for h in header_row]))
        of.write("\n")

        sql = """
            select rs.roi_id, rg.starttime, rg.endtime, var.name, rs.count, rs.sum, rs.mean, rs.stddev,
                rs.min, rs.max, rg.iarea, rg.type, rs.roi_name, rg.center_lat, rg.center_lon
            from forecast_roi_stats rs
            join variable var on var.id=rs.variable_id
            join forecast_roi_geoms rg on rg.id=rs.roi_id
            order by rs.roi_id asc, var.name asc
        """

        rows = pgdb_helper.query(sql)
        prev_roi_id = rows[0][0]
        roi_data = {
            "roi_id": prev_roi_id,
            "starttime": rows[0][1],
            "endtime": rows[0][2],
            "iarea": rows[0][10],
            "type": rows[0][11],
            "roi_name": rows[0][12],
            "lat": rows[0][13],
            "lon": rows[0][14]
        }

        count = 1
        for row in rows:
            if row[0] <> prev_roi_id or count == len(rows):
                roi_data["iarea"] = -1
                row_output = "%s, %s, %s, %d, %f, %f, %f," % (roi_data["roi_name"], roi_data["starttime"],
                            roi_data["endtime"], roi_data["type"], roi_data["iarea"], roi_data["lat"],
                            roi_data["lon"])
                for var_name in var_names:
                    if var_name in roi_data:
                        vd = roi_data[var_name]
                        row_output += "%d, %f, %f, %f, %f, %f," % (vd[4], vd[5], vd[6], vd[7], vd[8], vd[9])
                    else:
                        row_output += " %d, %f, %f, %f, %f, %f," % \
                                (missing_data, missing_data, missing_data, missing_data, missing_data, missing_data)

                of.write(row_output[:-1])
                of.write("\n")

                prev_roi_id = row[0]
                roi_data = {
                    "roi_id": prev_roi_id,
                    "starttime": row[1],
                    "endtime": row[2],
                    "iarea": -1, # row[11],
                    "type": row[11],
                    "roi_name": row[12],
                    "lat": row[13],
                    "lon": row[14],
                    row[3]: row
                }
            else:
                roi_data[row[3]] = row

            count += 1


if __name__ == '__main__':
    fname = "output.pred.csv"
    generate_output_file(fname)
