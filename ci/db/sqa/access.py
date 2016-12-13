from sqlalchemy.orm import sessionmaker
import geoalchemy2 as ga2
from ci.db.sqa.models import DataFormat, Provider, Variable, DataGranule, RasterTile, RoiGeom, RoiStat

__author__ = 'ujjwal'


def map_entity(entity_type):
    entity_type = str(entity_type).lower()
    if entity_type == 'dataformat':
        return DataFormat
    if entity_type == 'provider':
        return Provider
    if entity_type == 'variable':
        return Variable
    if entity_type == 'rastertile':
        return RasterTile
    if entity_type == 'datagranule':
        return DataGranule
    if entity_type == 'roi_geom':
        return RoiGeom
    if entity_type == 'roi_stat':
        return RoiStat
    return None


class SqaAccess(object):

    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.session.close()

    def _construct_query(self, entity_type, filterr={}):
        if isinstance(entity_type, str) or isinstance(entity_type, unicode):
            entity_type = map_entity(entity_type)
            if entity_type is None:
                raise Exception("wrong entity type %s" % entity_type)

        if len(filterr) > 0:
            str_filter = {}
            for key in filterr:
                str_filter[str(key)] = filterr[key]

            query = self.session.query(entity_type).filter_by(**str_filter)
        else:
            query = self.session.query(entity_type)

        return query

    def find(self, entity_type, filterr={}):
        query = self._construct_query(entity_type, filterr)
        return query.all()

    def findOne(self, entity_type, filterr={}):
        query = self._construct_query(entity_type, filterr)
        return query.one()

    #find and delete is much slower compared to delete
    def find_and_delete(self, entity_type, id):
        if isinstance(entity_type, str) or isinstance(entity_type, unicode):
            entity_type = map_entity(entity_type)
        idfilter = {
            'id': id
        }
        query = self.session.query(entity_type).filter_by(**idfilter)
        query.delete()
        self.session.commit()

    def delete(self, entity):
        self.session.delete(entity)
        self.session.commit()

    def byId(self, entity_type, id):
        if isinstance(entity_type, str) or isinstance(entity_type, unicode):
            entity_type = map_entity(entity_type)

        idfilter = {
            'id': id
        }
        query = self.session.query(entity_type).filter_by(**idfilter)
        entity = query.all()[0]
        return entity

    def insertMany(self, entities):
        self.session.add_all(entities)
        self.session.commit()

    def insertOne(self, entity):
        self.session.add(entity)
        self.session.commit()
        try:
            _id = entity.id
        except:
            _id = None
        return _id

    def get_dataformats(self):
        results = self.session.query(DataFormat).all()
        return results

    def get_variables(self):
        results = self.session.query(Variable).all()
        return results

    def get_providers(self):
        results = self.session.query(Provider).all()
        return results

    def upsert_datagranule(self, provider, variable, granule_name, start_time, end_time, extent, level, srid, table_name):

        granule = DataGranule(provider=provider, variable=variable, starttime=start_time, endtime=end_time,
                              extent=extent, level=level, name=granule_name, srid=srid, table_name=table_name,
                              file_name=table_name)

        check_granule_result = self.find(DataGranule,
                filterr={'provider_id': provider.id, 'variable_id': variable.id, 'level': level,
                         'starttime': start_time, 'endtime': end_time})

        if len(check_granule_result):
            check_granule = check_granule_result[0]
            check_extent_result = self.GeomEquals(extent, check_granule.extent)

            if check_extent_result:
                #logger.warn('found existing datagranule %d' % check_granule.id)
                rastertile_results =self.find('rastertile', filterr={'datagranule_id': check_granule.id})
                for rastertile in rastertile_results:
                    #logger.warn('removing existing rastertile %d' % rastertile.id)
                    self.delete(rastertile)

                self.delete(check_granule)

        self.insertOne(granule)

        return granule

    def AsEWKT(self, wkbelem, srid):
        return self.session.scalar(ga2.functions.ST_AsEWKT(ga2.functions.ST_Transform(wkbelem, srid)))

    def Intersects(self, geom1, geom2):
        return self.session.scalar(ga2.functions.ST_Intersects(geom1, geom2))

    def GeomEquals(self, geom1, geom2):
        return  self.session.scalar(ga2.functions.ST_Equals(geom1, geom2))