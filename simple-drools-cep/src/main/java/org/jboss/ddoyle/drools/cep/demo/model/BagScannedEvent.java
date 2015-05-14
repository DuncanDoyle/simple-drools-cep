package org.jboss.ddoyle.drools.cep.demo.model;

import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Event fired when a bag is scanned.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class BagScannedEvent extends AbstractFact implements Event {

	/**
	 * SerialVersionUID. 
	 */
	private static final long serialVersionUID = 1L;
	
	private final BagTag bagTag;
	
	private final Location location;
	
	private Date timestamp;
	
	public BagScannedEvent(BagTag bagTag, Location location) {
		this(bagTag, location, new Date());
	}
	
	public BagScannedEvent(BagTag bagTag, Location location, Date eventTimestamp) {
		this(UUID.randomUUID().toString(), bagTag,location, eventTimestamp);
	}
	
	public BagScannedEvent(String id, BagTag bagTag, Location location, Date eventTimestamp) {
		super(id);
		this.bagTag = bagTag;
		this.location = location;
		this.timestamp = eventTimestamp;
	}

	public Location getLocation() {
		return location;
	}

	public BagTag getBagTag() {
		return bagTag;
	}

	@Override
	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date eventTimestamp) {
		this.timestamp = eventTimestamp;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("bagTag", bagTag).append("location", location).append("timestamp", timestamp).toString();
	}
	
	
	
}
