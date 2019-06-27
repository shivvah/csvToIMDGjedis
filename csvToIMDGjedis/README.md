# csvToIMDG

Command Line Interface to read data from Comma Seperated Value file, identify an index as provided by the user, insert into an In Memory Data Grid.
<br />
Usage: csvToIMDG [-hvV] [-hz=<hzConfig>] [-if=<indexField>] [-it=<indexType>]<br />
                 [-mn=<mapName>] <file><br />
Imports csv files into IMDG Maps, assuming first field is index<br />
      <file>      The File name of source<br />
  -h, --help      Show this help message and exit.<br />
      -hz, --Hazelcast=<hzConfig><br />
                  The Hazelcast Client XML File<br />
      -if, --indexField=<indexField><br />
                  The field to use an index<br />
      -it, --indexType=<indexType><br />
                  The java type for the index Field<br />
      -mn, --mapName=<mapName><br />
                  The Name of the Map to Populate<br />
  -v, --verbose   Be verbose.<br />
  -V, --version   Print version information and exit.<br />
<br />
<br />
Sample execution: java -jar target/csvToIMDG-1.0-SNAPSHOT-jar-with-dependencies.jar -if Name -mn Sales -hz hazelcast-client.xml ../SalesJan2009.csv<br />
<br />
Feb 08, 2019 1:47:26 PM com.hazelcast.core.LifecycleService<br />
INFO: hz.client_0 [dev] [3.11.1] HazelcastClient 3.11.1 (20181218 - d294f31) is CLIENT_CONNECTED<br />
Feb 08, 2019 1:47:26 PM com.hazelcast.internal.diagnostics.Diagnostics<br />
INFO: hz.client_0 [dev] [3.11.1] Diagnostics disabled. To enable add -Dhazelcast.diagnostics.enabled=true to the JVM arguments.<br />
simone: {Transaction_date=1/13/09 1:46, Product=Product1, Price=1200, Payment_Type=Mastercard, Name=simone, City=Lyngby, State=Kobenhavn, Country=Denmark, Account_Created=10/30/07 12:03, Last_Login=2/5/09 8:52, Latitude=55.7666667, Longitude=12.5166667}
<br />
...
<br />
From IMDG: hazelcast[Sales] > m.get simone<br />
{Transaction_date=1/13/09 1:46, Product=Product1, Price=1200, Payment_Type=Mastercard, Name=simone, City=Lyngby, State=Kobenhavn, Country=Denmark, Account_Created=10/30/07 12:03, Last_Login=2/5/09 8:52, Latitude=55.7666667, Longitude=12.5166667}
<br />
<br />
As an example of using https://catalog.data.gov/dataset/crimes-2001-to-present-398a4 you can pair that with tomtom maps api and render the locations of all Murders in Chicago Area as reported from the above dataset:
<br />
![](./images/chicagomurders.png)
