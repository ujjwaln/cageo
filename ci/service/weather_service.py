import json
import os
from datetime import datetime
import dateutil.parser
from sqlalchemy import create_engine
from flask import Flask, Response, request
from ci.service import crossdomain, new_alchemy_encoder
from ci.db.sqa.access import SqaAccess
from ci.db.sqa.mapper import Mapper
from ci.db.sqa.models import RoiGeom, RoiStat
from ci.db.pgdbhelper import PGDbHelper
from ci.config import get_instance


__author__ = 'ujjwal'


app = Flask(__name__)


@app.route('/', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def index():
    response = Response("", mimetype="application/json")
    return response


@app.route('/config', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def config():
    from ci.config import get_instance
    conf = get_instance()
    data = {
        "start_date": conf.start_date,
        "end_date": conf.end_date
    }
    obj = json.dumps(data, cls=new_alchemy_encoder())
    return Response(obj, mimetype="application/json")


@app.route('/roi_geoms1', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def roi_geoms1():
    types = []
    results = []
    starttime = None
    endtime = None
    roi_name = None

    if "types" in request.args:
        types = json.loads(request.args.get("types"))

    if "starttime" in request.args:
        str_starttime = str(request.args.get("starttime"))
        starttime = datetime.strptime(str_starttime, '%Y-%m-%dT%H:%M:%S.%fZ')

    if "endtime" in request.args:
        str_endtime = str(request.args.get("endtime"))
        endtime = datetime.strptime(str_endtime, '%Y-%m-%dT%H:%M:%S.%fZ')

    if "name" in request.args:
        roi_name = str(request.args.get("name"))

    with SqaAccess(engine) as sqa:
        query = sqa.session.query(RoiGeom)
        if len(types) > 0:
            query = query.filter(RoiGeom.type.in_(types))
        if not starttime is None:
            query = query.filter(RoiGeom.starttime >= starttime)
        if not endtime is None:
            query = query.filter(RoiGeom.endtime <= endtime)
        if not roi_name is None:
            query = query.filter(RoiGeom.roi_name == roi_name)

        results = query.all()

    json_obj = json.dumps(results, cls=new_alchemy_encoder())
    return Response(json_obj, mimetype="application/json")


@app.route('/roi_geoms', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def roi_geoms():
    types = []
    results = []
    starttime = None
    endtime = None
    roi_name = None

    if "types" in request.args:
        types = json.loads(request.args.get("types"))
    if "starttime" in request.args:
        str_starttime = str(request.args.get("starttime"))
        starttime = datetime.strptime(str_starttime, '%Y-%m-%dT%H:%M:%S.%fZ')
    if "endtime" in request.args:
        str_endtime = str(request.args.get("endtime"))
        endtime = datetime.strptime(str_endtime, '%Y-%m-%dT%H:%M:%S.%fZ')
    if "name" in request.args:
        roi_name = str(request.args.get("name"))

    sql = """
        select id, roi_name, mrms_granule_id, starttime, endtime, st_asgeojson(geom), center_lat, center_lon, type
        from roi_geoms
    """

    filters = []
    if len(types):
        filters.append(" type in (%s) " % (",".join([str(t) for t in types])))
    if starttime and endtime:
        filters.append(" (starttime, endtime) overlaps ('%s', '%s') " % (starttime, endtime))
    if roi_name:
        filters.append(" roi_name=''" % roi_name)

    if len(filters):
        sql = sql + " where %s" % (" AND ".join(filters))

    results = pgdb_access.query(sql)
    objs = []
    for r in results:
        objs.append({
            "id": r[0],
            "roi_name": r[1],
            "granule_id": r[2],
            "starttime": r[3],
            "endtime": r[4],
            "geom": r[5],
            "lat": r[6],
            "lon": r[7],
            "type": r[8]
        })

    json_obj = json.dumps(objs, cls=new_alchemy_encoder())
    return Response(json_obj, mimetype="application/json")


@app.route('/<entity_name>', defaults={'entity_id': None}, methods=['GET', 'OPTIONS'])
@app.route('/<entity_name>/<entity_id>', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def get_entity(entity_name, entity_id=None):
    with SqaAccess(engine) as sqa_access:
        if entity_id is None:
            str_query = request.args.get("q", "{}")
            query = json.loads(str_query)
            objs = sqa_access.find(entity_name, query)
            json_obj = json.dumps(objs, cls=new_alchemy_encoder())
        else:
            entity_id = int(entity_id)
            obj = sqa_access.byId(entity_name, entity_id)
            json_obj = json.dumps(obj, cls=new_alchemy_encoder())

    return Response(json_obj, mimetype="application/json")


@app.route('/granule_search', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*', headers=["Content-Type"])
def granule_search():

    str_post_data = request.data
    post_data = json.loads(str_post_data)

    provider_id = post_data.get("provider_id", None)
    provider_id = int(provider_id)

    variable_id = post_data.get("variable_id", None)
    variable_id = int(variable_id)

    start_time = post_data.get("start_time", None)
    start_time = str(start_time)
    #start_time = dateutil.parser.parser(start_time)

    end_time = post_data.get("end_time", None)
    end_time = str(end_time)
    #end_time = dateutil.parser.parse(end_time)

    level = -1

    id_rows = pgdb_access.search_granules(provider_id=provider_id, variable_id=variable_id,
                                          start_time=start_time, end_time=end_time, level=level)
    ids = [r[0] for r in id_rows]
    with SqaAccess(engine) as sqa_access:
        granules = []
        for id in ids:
            granule = sqa_access.byId('datagranule', id)
            granules.append(granule)

        json_obj = json.dumps(granules, cls=new_alchemy_encoder())

    return Response(json_obj, mimetype="application/json")


@app.route('/raster', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def raster():
    str_granule = request.args.get("granule", None)
    granule = json.loads(str_granule)

    str_roi = request.args.get("roi", None)
    #roi = json.loads(str_roi)

    limit = int(request.args.get("limit", -1))
    min_val = request.args.get("min", None)
    max_val = request.args.get("max", None)
    if not min_val is None:
        try:
            min_val = float(min_val)
        except Exception:
            min_val = None
            pass

    if not max_val is None:
        try:
            max_val = float(max_val)
        except Exception:
            max_val = None
            pass

    if not granule is None:
        rows = pgdb_access.get_rastertiles_as_polygons(granule, str_roi, 4326, limit, min_val, max_val)
        results = []
        id = 0
        for row in rows:
            strgeom = row[0]
            value = row[1]
            geomObj = json.loads(strgeom)
            results.append({
                "id": id,
                "properties": {
                    "value": value
                },
                "geometry": geomObj,
                "type": "Feature"
            })
            id += 1
            min_val = min(value, min_val)
            max_val = max(value, max_val)

        featureSet = {
            "type": "FeatureCollection",
            "features": results,
            "min": min_val,
            "max": max_val
        }

        json_results = json.dumps(featureSet)
        response = Response(json_results, mimetype="application/json")
        return response


@app.route('/regrid', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def regrid():

    str_granule = request.args.get("granule", None)
    granule = json.loads(str_granule)

    #roi param holds the wkt string
    str_roi = request.args.get("roi", None)
    #roi = json.loads(str_roi)

    top = request.args.get("top", None)
    top = float(top)

    bottom = request.args.get("bottom", None)

    left = request.args.get("left", None)
    left = float(left)

    right = request.args.get("right", None)

    limit = int(request.args.get("limit", -1))
    min_val = request.args.get("min", 1e10)
    max_val = request.args.get("max", -1e10)

    resolution_x = request.args.get("res_x", 0.01)
    resolution_y = request.args.get("res_y", 0.01)

    resolution_y = float(resolution_y)
    resolution_x = float(resolution_x)

    if not granule is None:
        rows = pgdb_access.regrid(granule["id"], roi=str_roi, ul_lat=top, ul_lon=left,
                                  scale_x=resolution_x, scale_y=resolution_y, skew_x=0,skew_y=0)
        results = []
        id = 0
        for row in rows:
            strgeom = row[0]
            value = row[1]
            geomObj = json.loads(strgeom)
            results.append({
                "id": id,
                "properties": {
                    "value": value
                },
                "geometry": geomObj,
                "type": "Feature"
            })
            id += 1
            min_val = min(value, min_val)
            max_val = max(value, max_val)

        featureSet = {
            "type": "FeatureCollection",
            "features": results,
             "min": min_val,
            "max": max_val
        }

        json_results = json.dumps(featureSet)
        response = Response(json_results, mimetype="application/json")
        return response


@app.route('/create_roi', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def create_roi():
    str_granule = request.args.get("granule", None)
    granule = json.loads(str_granule)
    str_radius = request.args.get("radius", 30)
    radius = int(str_radius)
    threshold = int(request.args.get("threshold", 30))

    if not granule is None:
        rows = pgdb_access.get_rois_geojson(granule_id=granule["id"], threshold=threshold, radius=radius, srid=4326)
        results = []
        id = 0
        min_val = 1e9
        max_val = -1e9
        for row in rows:
            strgeom = row[0]
            value = row[1]
            geomObj = json.loads(strgeom)
            results.append({
                "id": id,
                "properties": {
                    "value": value
                },
                "geometry": geomObj,
                "type": "Feature"
            })
            id += 1
            min_val = min(value, min_val)
            max_val = max(value, max_val)

        featureSet = {
            "type": "FeatureCollection",
            "features": results,
            "min": min_val,
            "max": max_val
        }

        json_results = json.dumps(featureSet)
        response = Response(json_results, mimetype="application/json")
        return response


@app.route('/tiled/<granule_id>/<z>/<x>/<y>.png', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def tile(granule_id, z, x, y):
    granule_id = int(granule_id)
    x = int(x)
    y = int(y)
    z = int(z)
    tile_file_name = os.path.join(tiles_dir, "t_%d_%d_%d_%d.png" % (granule_id, z, x, y))

    if os.path.exists(tile_file_name):
        with open(tile_file_name, mode='rb') as f:
            png_data = f.read()
            response = Response(png_data, mimetype="image/png")
            return response

    return '', 204


if __name__ == '__main__':

    config = get_instance()

    #config_file = os.path.join("/home/ujjwal/DPR_SM/python/dpr_sm/ingest/lis_config.yml")
    #config = get_instance(config_file=config_file)

    engine = create_engine(config.sqa_connection_string())

    mapper = Mapper(engine=engine)
    mapper.map_tables()

    pgdb_access = PGDbHelper(config.pgsql_conn_str(), echo=config.logsql)
    pgdb_access.ensure_gist_index('rastertile', 'rastertile_rast_gist_idx', 'rast')
    pgdb_access.ensure_datagranule_id_index("rastertile", "rastertile_datagranule_id_idx", "datagranule_id")

    #check and create tile cache dir if necessary
    tiles_dir = os.path.join(os.path.dirname(__file__), "tiles")
    if not os.path.exists(tiles_dir):
        os.mkdir(tiles_dir)

    app.run(port=5001)
