# Project Atlas - São Paulo :earth_americas:

**Project Atlas - São Paulo** is a Data Science and Engineering initiative that aims at developing *relevant and curated Geospatial features* about the city of São Paulo, Brazil. It's ultimate use is varied, but it is mainly focused on Machine Learning tasks, such as Real State price prediction.

It aggregates several attributes from many public data sources at different *levels of interest*, which can be used to match geospatially referenced data (`lat`,`long` pairs for example).

A breakdown of the data sources currently used and their original references can be found below, but the official documentation of the project contains the full list of data sources.

# Data sources
- [GeoSampa](http://geosampa.prefeitura.sp.gov.br/PaginasPublicas/_SBC.aspx): geospatial data exploration tool provided by the São Paulo's Department of Urban Development;
- [IBGE](https://downloads.ibge.gov.br/): raw datasets from the 2010 Census conducted by the Brazilian Institute of Geography and Statistics;
- [Infocidade](http://infocidade.prefeitura.sp.gov.br/): multiple sources from the city of São Paulo's local government entities;
- [São Paulo Open Data Portal](http://dados.prefeitura.sp.gov.br/pt_PT/): multiple curated data sources from the city of São Paulo;

# Technologies used
The main technologies used in this project were:
1. Data processing: `Apache Spark`, `pyspark`;
2. Geospatial data wrangling: `Apache Sedona`, `geopandas`, `fiona`;
3. Data Versioning: `dvc`

# Project Architecture 

The project is broken down by different levels of granularities of Geospatial references. These are:

1. **Census Sectors**: the lowest level of census information, that can be roughly approximated to a street block;
2. **Zip code** different from a block, the zip code can be roughly approximated to a streets;
3. **Area of Ponderation**: areas of ponderation are aggregations of census sectors, which vary in size. The level of interest is important as some of the data sources in the project are only described in such level;
4. **Neighborhoods**: neighborhoods are not formally defined and place in between `districts` and `areas of ponderation` when it comes to size;
5. **Districts**: districts are administrative regions in the city of São Paulo, which is defined formally by law (with that, their boundaries do not change that much over time);

The map below iilustrates this relationship:

![Example map for Levels of Interest](references/img/example_layers.png)

> Note: The outer red line indicates the `districts`. The green line represents the `neighborhoods`. The blue lines indicate `zipcodes`, as so on.