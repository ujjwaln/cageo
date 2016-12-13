from sqlalchemy import func
from sqlalchemy.types import UserDefinedType

__author__ = 'ujjwal'


class GeoJsonGeometryType(UserDefinedType):
    def get_col_spec(self):
        return "GEOMETRY"

    def bind_expression(self, bindvalue):
        return func.ST_GeomFromEWKT(bindvalue, type_=self)

    def column_expression(self, colexpr):
        return func.ST_AsGeoJSON(colexpr, type_=self)


class PGGeometryType(UserDefinedType):
    def get_col_spec(self):
        return "GEOMETRY"

    def bind_expression(self, bindvalue):
        #return func.ST_GeomFromText(bindvalue, type_=self)
        return func.ST_GeomFromEWKT(bindvalue, type_=self)

    def column_expression(self, colexpr):
        #return func.ST_AsText(colexpr, type_=self)
        return func.ST_AsEWKT(colexpr, type_=self)


class PGRasterType(UserDefinedType):
    def get_col_spec(self):
        return "RASTER"

    def column_expression(self, colexpr):
        ssql = func.ST_AsBinary(colexpr)
        return ssql

    # def bind_expression(self, bindvalue):
    #     sql = func.ST_AsText(bindvalue)
    #     return sql