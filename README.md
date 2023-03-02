# HiveConnector
Connect to Hive from Scala Spark

Apart from the code the following steps were taken
shlpndpdh038 is configured to use udp is not allowing outside connection.
Changing it use tcp will solve issue. Also removed ticket renewing
remove renew_lifetime = 7d from krb5.conf
add udp_preference_limit=1 under [libdefaults]

Also, the main server was added to etc/hosts

krb5.conf and keytab file was copied over to local.
