#How to store the primary key

##Problem
You want to store the value of the primary key in the Aerospike database.

##Solution
set you write and read policies' `sendKey` to true;

```java
this.client.writePolicyDefault.sendKey = true;
this.client.readPolicyDefault.sendKey = true;
this.client.scanPolicyDefault.sendKey = true;
```
## How to build
Clone the git gub repository with
```bash
git clone https://github.com/helipilot50/store-primary-key
```
To build this example, use:
```bash
mvn clean package
```

