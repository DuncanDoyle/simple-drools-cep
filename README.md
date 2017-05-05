# simple-drools-cep
A simple Drools Fusion CEP example.

The demo loads a set of events (Airport bag-scanning events) from a CVS file.

For reproducability purposes, we use a, so called, PseudoClock instead of the Drools realtime clock. We advance the clock programmatically based on the timestamp of the events.
This allows us to:
* reproduce the same output over and over again because we don't depend on the actual system clock. The time advances deterministically.
* not depend on the actual progression of time. We can replay events that occur minutes, hours or even days apart in a number of seconds.

To run the demo, simply run the `org.jboss.ddoyle.drools.cep.demo.Main` class.

  mvn clean package exec:java
