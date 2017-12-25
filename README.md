# image-datapipeline

#Ideally in requirements.txt

Install PySpark from using
`pip install pyspark`
Install snakebite using
`pip install snakebite`

Size is hardcoded to 300 x 300

Program assumes the Input directory is in hdfs and of structure
`hdfs://app/INPUTDIR/CLASS/FILES.*`

Output of program will be visible in
`hdfs://app/OUTPUTDIR/CLASS/(training|testing|validation)/FILES.JPG`

Locally Run using `bash run_pipeline.sh INPUTDIR OUTPUTDIR`

