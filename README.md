# LotCadastre(NSW)-Planning(Zoning) Intersection Incremental Update

<!-- TABLE OF CONTENTS -->
## Table of Contents
1. [Background](#background)
2. [Built With](#built-with)
3. [Concept](#concept)


## Background
The NSW Lot Cadastre [link](https://maps.six.nsw.gov.au/arcgis/rest/services/sixmaps/Cadastre/MapServer/0) is a live Geospatial layer depicting the legal boundaries of land parcels in New South Wales, Australia. It has over 3 million records and is maintained by Spatial Services (Department of Customer Services).

The Land Zoning [link](https://mapprod3.environment.nsw.gov.au/arcgis/rest/services/Planning/EPI_Primary_Planning_Layers/MapServer/2) layer is a Geospatial dataset that identifies land use zones and the type of land uses that are permitted (with or without consent) or prohibited in each zone on any given land as designated by the relevant NSW environmental planning instrument (EPI) under the Environmental Planning and Assessment Act 1979.

There is a business need for the intersection results of these 2 datasets as it provides valuable insights for land use strategy and planning especially for large scale analysis. Currently, intersections will be performed on a subset of both data sets and on an ad-hoc basis. This proposed solution aims to maintain a dataset that requires minimal maintenance and provides quick and easy access to up to date Lot-Zoning intersection information.

## Built With

* Python
* Arcpy
* Oracle

## Concept
![Lot-Planning_Zone-Intersect v4 drawio](https://github.com/Pooomr/LotCadastre-Planning-Intersect-Incremental-Update/assets/140774543/6669a6c1-cf61-4577-ab98-4f1a18bfddea)
