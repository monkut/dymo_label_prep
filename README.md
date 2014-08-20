dymo_label_prep
===============


From OSM (open-street-map) pbf "osm2pgsql" loaded data, create label-z{level} CSV input data for https://github.com/migurski/Dymo label-positioning tool


Usage:

    python create_csv_zoom_labels.py --dbname DBNAME --user DBUSER --password DBPASS --name_column name:ja


WHERE:

    name:ja is the language column to use (defaults to "name").
    
    
NOTE:  
'name:ja' or other languages need to be loaded to the target database by adding them to the default.style/osm2pgsql.style file used for loading the OSM pbf/xml data to the database.

Sample osm2pgsql load command:

    sudo -u postgres osm2pgsql -c -G -S ./osm2pgsql.style -d osm_db ~/maps/japan/japan-latest.osm.pbf -C 22000

    
