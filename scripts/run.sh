#python python/generate.py python/configs/config1.ini
#mvn exec:java
#cp reports/report.txt reports/report1.txt
#cp -R results results1

#python python/generate.py python/configs/config2.ini
#mvn exec:java
#cp reports/report.txt reports/report2.txt
#cp -R results results2

#python python/generate.py python/configs/config3.ini
#mvn exec:java
#cp reports/report.txt reports/report3.txt
#cp -R results results3

python python/generate.py python/configs/config4.ini
mvn exec:java
cp reports/report.txt reports/report4.txt
cp -R results results4
