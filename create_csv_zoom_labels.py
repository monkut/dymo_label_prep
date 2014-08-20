# -*- coding: utf-8 -*-
'''
From OSM (open-street-map) pbf "osm2pgsql" loaded data,
create label-z{level} CSV input data for dymo label-positioning tool
'''
import os
import csv
import math
from math import radians, degrees, pi, cos, tan, log, atan, sinh

from multiprocessing import Pool, cpu_count


import psycopg2
from django.contrib.gis.geos import GEOSGeometry, Point

DEFAULT_MAX_CITIES_PER_TILE = 7
DEFAULT_FONT_FILE = "./fonts/Arial-Unicode-Bold.ttf"  # font that supports Japanese or target language
DEFAULT_ZOOM_LEVEL_FONT_SIZES = {
                                 # zoom: font-size
                                 0: 10,
                                 1: 10,
                                 2: 10,
                                 3: 10,
                                 4: 10,
                                 5: 12,
                                 6: 12,
                                 7: 12,
                                 8: 13,
                                 9: 13,
                                 10: 13,
                                 11: 13,
                                 12: 13,
                                 13: 13,
                                 14: 13,
                                 15: 13,
                                 16: 13,
                                 17: 13,
                                 18: 13,
                                 19: 13,
                                 20: 13,
                                 21: 13,
                                 22: 13,
                                 }

WGS84_SRID = 4326
GOOGLE_SRID = 3857#900913





class Tilelator(object):
    
    def __init__(self, extents, zoom=None, tile_size=256):
        assert extents 
        assert zoom is not None
        self.zoom = zoom
        xmin, ymin, xmax, ymax = extents
        upperleft = Point(xmin, ymax, srid=GOOGLE_SRID)
        upperleft.transform(WGS84_SRID)
        
        lowerright = Point(xmax, ymin, srid=GOOGLE_SRID)
        lowerright.transform(WGS84_SRID)
        
        upperleft_tilex, upperleft_tiley = self.lonlatdeg2tilexy(upperleft.x, upperleft.y, zoom)
        lowerright_tilex, lowerright_tiley = self.lonlatdeg2tilexy(lowerright.x, lowerright.y, zoom)
        self.upperleft_tile = Point(upperleft_tilex, upperleft_tiley)
        self.lowerright_tile = Point(lowerright_tilex, lowerright_tiley)
        
        
        self.tile_size = tile_size
        self.initial_resolution = 2 * pi * 6378137 / self.tile_size
        # 156543.03392804062 for tileSize 256 pixels
        self.origin_shift = 2 * pi * 6378137 / 2.0
        # 20037508.342789244   
        
    def resolution(self, zoom):
        return self.initial_resolution / (2**zoom)
        
    def pixel2meters(self, px, py, zoom):
        res = self.resolution(zoom)
        meters_x = px * res - self.origin_shift
        meters_y = abs(py * res - self.origin_shift)
        return meters_x, meters_y
        
    def tile_bounds(self, tilex, tiley, zoom):
        min_pixel_x = tilex * self.tile_size
        max_pixel_y = tiley * self.tile_size
        max_pixel_x = (tilex + 1) * self.tile_size
        min_pixel_y = (tiley + 1) * self.tile_size
        minx, miny = self.pixel2meters(min_pixel_x, min_pixel_y, zoom)
        maxx, maxy = self.pixel2meters(max_pixel_x, max_pixel_y, zoom)        
        return minx, miny, maxx, maxy        
        
        
        
    def iterate_tiles(self):
        for tilex in range(int(self.upperleft_tile.x), int(self.lowerright_tile.x + 1)):
            for tiley in range(int(self.upperleft_tile.y), int(self.lowerright_tile.y + 1)):
                yield self.tile_bounds(tilex, tiley, self.zoom)
        
        
    def sec(self, x):
        return(1/cos(x))
    
    def lonlatdeg2tilexy(self, lon_deg, lat_deg, zoom):
        lat_rad = math.radians(lat_deg)
        n = 2.0 ** zoom
        xtile = int((lon_deg + 180.0) / 360.0 * n)
        ytile = int(((1 - log(tan(lat_rad) + self.sec(lat_rad)) / pi) / 2) * n)
        return xtile, ytile
    
    def tilexy2lonlatdeg(self, xtile, ytile, zoom):
        n = 2.0 ** zoom
        lon_deg = xtile / n * 360.0 - 180.0
        lat_rad = atan(sinh(pi * (1 - 2 * ytile / n)))
        lat_deg = degrees(lat_rad)
        return lon_deg, lat_deg        
    
                
                
def rescale(value, in_min, in_max, out_min, out_max):
    new_value = (((value - in_min) * (out_max - out_min)) / float((in_max - in_min)) + out_min)
    return new_value                
        

def process_zoom_level(arguments):
    zoom, options = arguments
    output_dirpath = options["output_dirpath"]
    zoom_level_filename = "city_labels_z{}.csv".format(zoom)
    output_csv_filepath = os.path.join(output_dirpath, zoom_level_filename)
    
    # postgis db options
    #dbname=osm_height host=localhost user=postgres password=postgres
    dbname = options["dbname"]
    host = options["host"]
    user = options["user"]
    password = options["password"]
    
    
    # dymo options
    dbname_column = options.get("name_column", "name")
    default_font_size = options.get("default_font_size", DEFAULT_ZOOM_LEVEL_FONT_SIZES[zoom])
    default_font_flie = options.get("default_font_file", DEFAULT_FONT_FILE) # 'Arial Unicode MS Bold'
    default_point_size = options.get("default_point_size", 0)
    preferred_placement = options.get("preferred_placement", "")
    
    # tile label option
    # --> determines number of cities to be output per 256x256 tile
    max_cities_per_tile = options.get("max_cities_per_tile", DEFAULT_MAX_CITIES_PER_TILE)    
    
    dymo_headers = (
                    "name", # the name of the feature you want to label
                    "latitude",# the latitude, in decimal degrees, of the point feature
                    "longitude", # the longitude, in decimal degrees, of the point feature
                    "font size", # the font size of the label you want placed
                    "font file", # the font type and location of the font file
                    "point size", # if you are symbolizing points to go along with your labels, the size of the points
                    "preferred placement", # if there is a particular label you want placed in a specific location around a point, you can add it here (example: top, right, left, etc.)
                    )
    with open(output_csv_filepath, "wb") as out_f, psycopg2.connect(database=dbname, host=host, user=user, password=password) as con:
        writer = csv.writer(out_f)
        writer.writerow(dymo_headers)
        cursor = con.cursor()
        
        max_population_sql = '''SELECT MAX(population::numeric) from planet_osm_point WHERE "{name_column}" IS NOT NULL and population IS NOT NULL AND place IN ('city', 'suburb', 'town', 'village');'''.format(name_column=dbname_column)
        cursor.execute(max_population_sql)
        max_population = float(cursor.fetchall()[0][0])
        
        # for each output tile determine the cities to be included for display        
        
        # -- determine the pixels per tile for the given zoom level
        # --> each tile is 256x256 pixels

        
        # -- Query data from db per tile & output city label results to CSV        
        tile_iterator = Tilelator(extents=options["extents"], zoom=zoom)
        
        if zoom >= 8:
            BIG_FONT_SIZE = 20
        elif 7 >= zoom >= 6:
            BIG_FONT_SIZE = 18
        elif zoom < 6:
            BIG_FONT_SIZE = 16
        
        # tx, ty in GOOGLE_SRID 
        for txmin, tymin, txmax, tymax in tile_iterator.iterate_tiles(): 
            top_cities_in_tile_query = ('''SELECT "{name_column}", "place", population::numeric, ST_AsEWKT(way) FROM planet_osm_point '''
                                        '''WHERE '''
                                        '''"{name_column}" IS NOT NULL AND '''
                                        '''population IS NOT NULL AND '''
                                        '''place IN ('city', 'suburb', 'town', 'village') AND  ''' 
                                        '''planet_osm_point.way && ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 3857) '''
                                        '''ORDER BY population::numeric DESC LIMIT {max_cities_per_tile}; ''').format(name_column=dbname_column,
                                                                                                                                       xmin=txmin,
                                                                                                                                       ymin=tymin,
                                                                                                                                       xmax=txmax,
                                                                                                                                       ymax=tymax,
                                                                                                                                       max_cities_per_tile=max_cities_per_tile)
            cursor.execute(top_cities_in_tile_query)
            for name_utf8, place, population, way_ewkt in cursor:
                way_point = GEOSGeometry(way_ewkt)
                if not way_point.srid or way_point.srid == 900913:
                    # use clean designation
                    way_point.srid = GOOGLE_SRID
                way_point.transform(WGS84_SRID)                
                
                
                # scale population value and adjust font size
                scaled_population = rescale(float(population), 0, max_population, 0, 100)
                if scaled_population > 30:
                    font_size = BIG_FONT_SIZE
                else:
                    font_size = default_font_size
                    
                row = (
                       name_utf8,
                       round(way_point.y, 5), 
                       round(way_point.x, 5),                        
                       font_size,
                       default_font_flie,
                       default_point_size,
                       preferred_placement,
                       )
                writer.writerow(row)
    return output_csv_filepath 


def get_db_extents(dbname, host, user, password):
    with psycopg2.connect(database=dbname, host=host, user=user, password=password) as con:
        cursor = con.cursor()
        # -- determine the extents of data in db.
        sql_query = "SELECT ST_AsEWKT(ST_Extent(way)) as table_extent FROM planet_osm_point;"
        cursor.execute(sql_query)
        results = cursor.fetchall()
        result_ewkt = results[0][0]
        g = GEOSGeometry(result_ewkt)
        g.srid = GOOGLE_SRID
        return g.extent
    
    
# TODO: create multiprocess pool to create each dymo csv file.

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dbname", "-d",
                        default=None,
                        required=True,
                        help="database name")
    parser.add_argument("--user", "-u",
                        default=None,
                        required=True,
                        help="database user")
    parser.add_argument("--password", "-p", 
                        default=None,
                        required=True,
                        help="database user password")
    parser.add_argument("--host",
                        default="localhost",
                        help="database host")
    parser.add_argument("--startzoom", "-s",
                        type=int,
                        default=8,
                        help="Start Zoom level to process, valid range: 0 to 22 [DEFAULT: 8")
    parser.add_argument("--endzoom", "-e",
                        type=int,
                        default=16,
                        help="End Zoom level to process, valid range: 0 to 22 [DEFAULT: 16")
    parser.add_argument("--extent",
                        default=None,
                        nargs="+",
                        help="Extents of area in Database to process. (minx, miny, maxx, maxy) [DEFAULT: Database extents]")
    parser.add_argument("--outputdir", "-o",
                        default=os.path.abspath("."),
                        help="output directory for resulting zoom CSV files. [DEFAULT: . ]")
    parser.add_argument("--max_cities_per_tile", "-m",
                        type=int,
                        default=DEFAULT_MAX_CITIES_PER_TILE,
                        help="Number of city labels to display per tile [DEFAULT: {}]".format(DEFAULT_MAX_CITIES_PER_TILE))
    parser.add_argument("--name_column", "-n", 
                        default="name",
                        help="Name column to use for labels (Allows for labels to be displayed in other languages, for example, 'name:ja', for japanese) [DEFAULT: 'name']")
    parser.add_argument("--processes",
                        default=cpu_count() -1,
                        help="processes to use [DEFAULT: {}]".format(cpu_count() -1),
                        )
    args = parser.parse_args()
    
    
    startz = args.startzoom
    endz = args.endzoom
    if not (0 <= startz < endz <= 22):
        raise ValueError("Error with --startzoom or --endzoom value! [0 <= startzoom({})) < endzoom({}) <= 22]".format(startz, endz, startz, endz))
     
    zoom_levels = range(startz, endz + 1)
    
    if not args.extent:
        extent = get_db_extents(args.dbname, args.host, args.user, args.password)
    else:
        extent = args.extent
        
    
    
    tasks = [(z, {"name_column": args.name_column, 
                  "output_dirpath": args.outputdir, 
                  "extents": extent,
                  "dbname": args.dbname,
                  "user": args.user,
                  "password": args.password,
                  "host": args.host,
                  "max_cities_per_tile": args.max_cities_per_tile}) for z in zoom_levels]
    total_tasks = len(tasks)
    print "extent: {}".format(extent)
    print "processes: {}".format(args.processes)
    print "Processing ({}) Zoom Levels...".format(total_tasks)
    if args.processes == 1:
        for task_count, task in enumerate(tasks, 1):
            result = process_zoom_level(task)
            print "({}/{}): {}".format(task_count, total_tasks, result)
    else:        
        p = Pool(args.processes)
        it = p.imap_unordered(process_zoom_level, tasks)
        for task_count, result in enumerate(it, 1):
            print "({}/{}): {}".format(task_count, total_tasks, result)
    
    
    
    
    
