create index on rastertile using gist(st_convexhull(rast));
create index on roi_geoms using gist(geom);
create index on roi_geoms using gist(center);
create index roi_geoms_granule_id_type_time_idx on roi_geoms (mrms_granule_id, type, starttime, endtime);
