# Translation-Data-Processing-for-Generalization-Models

As in Research for better generalizing AI models, the need for a good, robust, well builded infrastructures is quite firm.
This project is designed as an initial building block for data handling. With supporting Translation dictionary which enables changing\editing main data fields names, and a running differential crawling mechanism for more efficient, duplicants reduction, files scanning and handling.

This model includes: 
* *Caching LRU mechanism
* *Differential Files Crawling
* *Meta Data saving in SQL (in a differential manner) - including MD5 Hashing
* *Data Filtering
* *Data removing
* *Data Interpolation (Linear, Polynomial, Spline, std, etc.)
* *Missing Data handling
* *Blank Data filling
* *Corrupted data processing
* *SQL Table creating for Raw and a Clean data version
* *SQL Viewing Table with Translation dictionary support
* *fetching and querying SQL abilities

The handled data is mainly with Excel extensions with\without sheets division and with\without any spicific order for the data in each sheet.
The model can run as a stand-alone model and without any external parameters (although it's definitely an option).
