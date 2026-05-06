// ORM class for table 'yellow_taxi_trips'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Apr 11 14:00:48 MSK 2026
// For connector: org.apache.sqoop.manager.PostgresqlManager
package bigdata.team35.taxi;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.sqoop.lib.JdbcWritableBridge;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.lib.FieldFormatter;
import org.apache.sqoop.lib.RecordParser;
import org.apache.sqoop.lib.BooleanParser;
import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class yellow_taxi_trips extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("trip_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.trip_id = (Long)value;
      }
    });
    setters.put("vendorid", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.vendorid = (Integer)value;
      }
    });
    setters.put("tpep_pickup_datetime", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.tpep_pickup_datetime = (java.sql.Timestamp)value;
      }
    });
    setters.put("tpep_dropoff_datetime", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.tpep_dropoff_datetime = (java.sql.Timestamp)value;
      }
    });
    setters.put("passenger_count", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.passenger_count = (Double)value;
      }
    });
    setters.put("trip_distance", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.trip_distance = (Double)value;
      }
    });
    setters.put("ratecodeid", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.ratecodeid = (Double)value;
      }
    });
    setters.put("store_and_fwd_flag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.store_and_fwd_flag = (String)value;
      }
    });
    setters.put("pulocationid", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.pulocationid = (Integer)value;
      }
    });
    setters.put("dolocationid", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.dolocationid = (Integer)value;
      }
    });
    setters.put("payment_type", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.payment_type = (Double)value;
      }
    });
    setters.put("fare_amount", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.fare_amount = (Double)value;
      }
    });
    setters.put("extra", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.extra = (Double)value;
      }
    });
    setters.put("mta_tax", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.mta_tax = (Double)value;
      }
    });
    setters.put("tip_amount", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.tip_amount = (Double)value;
      }
    });
    setters.put("tolls_amount", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.tolls_amount = (Double)value;
      }
    });
    setters.put("improvement_surcharge", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.improvement_surcharge = (Double)value;
      }
    });
    setters.put("total_amount", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.total_amount = (Double)value;
      }
    });
    setters.put("congestion_surcharge", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.congestion_surcharge = (Double)value;
      }
    });
    setters.put("airport_fee", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        yellow_taxi_trips.this.airport_fee = (Double)value;
      }
    });
  }
  public yellow_taxi_trips() {
    init0();
  }
  private Long trip_id;
  public Long get_trip_id() {
    return trip_id;
  }
  public void set_trip_id(Long trip_id) {
    this.trip_id = trip_id;
  }
  public yellow_taxi_trips with_trip_id(Long trip_id) {
    this.trip_id = trip_id;
    return this;
  }
  private Integer vendorid;
  public Integer get_vendorid() {
    return vendorid;
  }
  public void set_vendorid(Integer vendorid) {
    this.vendorid = vendorid;
  }
  public yellow_taxi_trips with_vendorid(Integer vendorid) {
    this.vendorid = vendorid;
    return this;
  }
  private java.sql.Timestamp tpep_pickup_datetime;
  public java.sql.Timestamp get_tpep_pickup_datetime() {
    return tpep_pickup_datetime;
  }
  public void set_tpep_pickup_datetime(java.sql.Timestamp tpep_pickup_datetime) {
    this.tpep_pickup_datetime = tpep_pickup_datetime;
  }
  public yellow_taxi_trips with_tpep_pickup_datetime(java.sql.Timestamp tpep_pickup_datetime) {
    this.tpep_pickup_datetime = tpep_pickup_datetime;
    return this;
  }
  private java.sql.Timestamp tpep_dropoff_datetime;
  public java.sql.Timestamp get_tpep_dropoff_datetime() {
    return tpep_dropoff_datetime;
  }
  public void set_tpep_dropoff_datetime(java.sql.Timestamp tpep_dropoff_datetime) {
    this.tpep_dropoff_datetime = tpep_dropoff_datetime;
  }
  public yellow_taxi_trips with_tpep_dropoff_datetime(java.sql.Timestamp tpep_dropoff_datetime) {
    this.tpep_dropoff_datetime = tpep_dropoff_datetime;
    return this;
  }
  private Double passenger_count;
  public Double get_passenger_count() {
    return passenger_count;
  }
  public void set_passenger_count(Double passenger_count) {
    this.passenger_count = passenger_count;
  }
  public yellow_taxi_trips with_passenger_count(Double passenger_count) {
    this.passenger_count = passenger_count;
    return this;
  }
  private Double trip_distance;
  public Double get_trip_distance() {
    return trip_distance;
  }
  public void set_trip_distance(Double trip_distance) {
    this.trip_distance = trip_distance;
  }
  public yellow_taxi_trips with_trip_distance(Double trip_distance) {
    this.trip_distance = trip_distance;
    return this;
  }
  private Double ratecodeid;
  public Double get_ratecodeid() {
    return ratecodeid;
  }
  public void set_ratecodeid(Double ratecodeid) {
    this.ratecodeid = ratecodeid;
  }
  public yellow_taxi_trips with_ratecodeid(Double ratecodeid) {
    this.ratecodeid = ratecodeid;
    return this;
  }
  private String store_and_fwd_flag;
  public String get_store_and_fwd_flag() {
    return store_and_fwd_flag;
  }
  public void set_store_and_fwd_flag(String store_and_fwd_flag) {
    this.store_and_fwd_flag = store_and_fwd_flag;
  }
  public yellow_taxi_trips with_store_and_fwd_flag(String store_and_fwd_flag) {
    this.store_and_fwd_flag = store_and_fwd_flag;
    return this;
  }
  private Integer pulocationid;
  public Integer get_pulocationid() {
    return pulocationid;
  }
  public void set_pulocationid(Integer pulocationid) {
    this.pulocationid = pulocationid;
  }
  public yellow_taxi_trips with_pulocationid(Integer pulocationid) {
    this.pulocationid = pulocationid;
    return this;
  }
  private Integer dolocationid;
  public Integer get_dolocationid() {
    return dolocationid;
  }
  public void set_dolocationid(Integer dolocationid) {
    this.dolocationid = dolocationid;
  }
  public yellow_taxi_trips with_dolocationid(Integer dolocationid) {
    this.dolocationid = dolocationid;
    return this;
  }
  private Double payment_type;
  public Double get_payment_type() {
    return payment_type;
  }
  public void set_payment_type(Double payment_type) {
    this.payment_type = payment_type;
  }
  public yellow_taxi_trips with_payment_type(Double payment_type) {
    this.payment_type = payment_type;
    return this;
  }
  private Double fare_amount;
  public Double get_fare_amount() {
    return fare_amount;
  }
  public void set_fare_amount(Double fare_amount) {
    this.fare_amount = fare_amount;
  }
  public yellow_taxi_trips with_fare_amount(Double fare_amount) {
    this.fare_amount = fare_amount;
    return this;
  }
  private Double extra;
  public Double get_extra() {
    return extra;
  }
  public void set_extra(Double extra) {
    this.extra = extra;
  }
  public yellow_taxi_trips with_extra(Double extra) {
    this.extra = extra;
    return this;
  }
  private Double mta_tax;
  public Double get_mta_tax() {
    return mta_tax;
  }
  public void set_mta_tax(Double mta_tax) {
    this.mta_tax = mta_tax;
  }
  public yellow_taxi_trips with_mta_tax(Double mta_tax) {
    this.mta_tax = mta_tax;
    return this;
  }
  private Double tip_amount;
  public Double get_tip_amount() {
    return tip_amount;
  }
  public void set_tip_amount(Double tip_amount) {
    this.tip_amount = tip_amount;
  }
  public yellow_taxi_trips with_tip_amount(Double tip_amount) {
    this.tip_amount = tip_amount;
    return this;
  }
  private Double tolls_amount;
  public Double get_tolls_amount() {
    return tolls_amount;
  }
  public void set_tolls_amount(Double tolls_amount) {
    this.tolls_amount = tolls_amount;
  }
  public yellow_taxi_trips with_tolls_amount(Double tolls_amount) {
    this.tolls_amount = tolls_amount;
    return this;
  }
  private Double improvement_surcharge;
  public Double get_improvement_surcharge() {
    return improvement_surcharge;
  }
  public void set_improvement_surcharge(Double improvement_surcharge) {
    this.improvement_surcharge = improvement_surcharge;
  }
  public yellow_taxi_trips with_improvement_surcharge(Double improvement_surcharge) {
    this.improvement_surcharge = improvement_surcharge;
    return this;
  }
  private Double total_amount;
  public Double get_total_amount() {
    return total_amount;
  }
  public void set_total_amount(Double total_amount) {
    this.total_amount = total_amount;
  }
  public yellow_taxi_trips with_total_amount(Double total_amount) {
    this.total_amount = total_amount;
    return this;
  }
  private Double congestion_surcharge;
  public Double get_congestion_surcharge() {
    return congestion_surcharge;
  }
  public void set_congestion_surcharge(Double congestion_surcharge) {
    this.congestion_surcharge = congestion_surcharge;
  }
  public yellow_taxi_trips with_congestion_surcharge(Double congestion_surcharge) {
    this.congestion_surcharge = congestion_surcharge;
    return this;
  }
  private Double airport_fee;
  public Double get_airport_fee() {
    return airport_fee;
  }
  public void set_airport_fee(Double airport_fee) {
    this.airport_fee = airport_fee;
  }
  public yellow_taxi_trips with_airport_fee(Double airport_fee) {
    this.airport_fee = airport_fee;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof yellow_taxi_trips)) {
      return false;
    }
    yellow_taxi_trips that = (yellow_taxi_trips) o;
    boolean equal = true;
    equal = equal && (this.trip_id == null ? that.trip_id == null : this.trip_id.equals(that.trip_id));
    equal = equal && (this.vendorid == null ? that.vendorid == null : this.vendorid.equals(that.vendorid));
    equal = equal && (this.tpep_pickup_datetime == null ? that.tpep_pickup_datetime == null : this.tpep_pickup_datetime.equals(that.tpep_pickup_datetime));
    equal = equal && (this.tpep_dropoff_datetime == null ? that.tpep_dropoff_datetime == null : this.tpep_dropoff_datetime.equals(that.tpep_dropoff_datetime));
    equal = equal && (this.passenger_count == null ? that.passenger_count == null : this.passenger_count.equals(that.passenger_count));
    equal = equal && (this.trip_distance == null ? that.trip_distance == null : this.trip_distance.equals(that.trip_distance));
    equal = equal && (this.ratecodeid == null ? that.ratecodeid == null : this.ratecodeid.equals(that.ratecodeid));
    equal = equal && (this.store_and_fwd_flag == null ? that.store_and_fwd_flag == null : this.store_and_fwd_flag.equals(that.store_and_fwd_flag));
    equal = equal && (this.pulocationid == null ? that.pulocationid == null : this.pulocationid.equals(that.pulocationid));
    equal = equal && (this.dolocationid == null ? that.dolocationid == null : this.dolocationid.equals(that.dolocationid));
    equal = equal && (this.payment_type == null ? that.payment_type == null : this.payment_type.equals(that.payment_type));
    equal = equal && (this.fare_amount == null ? that.fare_amount == null : this.fare_amount.equals(that.fare_amount));
    equal = equal && (this.extra == null ? that.extra == null : this.extra.equals(that.extra));
    equal = equal && (this.mta_tax == null ? that.mta_tax == null : this.mta_tax.equals(that.mta_tax));
    equal = equal && (this.tip_amount == null ? that.tip_amount == null : this.tip_amount.equals(that.tip_amount));
    equal = equal && (this.tolls_amount == null ? that.tolls_amount == null : this.tolls_amount.equals(that.tolls_amount));
    equal = equal && (this.improvement_surcharge == null ? that.improvement_surcharge == null : this.improvement_surcharge.equals(that.improvement_surcharge));
    equal = equal && (this.total_amount == null ? that.total_amount == null : this.total_amount.equals(that.total_amount));
    equal = equal && (this.congestion_surcharge == null ? that.congestion_surcharge == null : this.congestion_surcharge.equals(that.congestion_surcharge));
    equal = equal && (this.airport_fee == null ? that.airport_fee == null : this.airport_fee.equals(that.airport_fee));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof yellow_taxi_trips)) {
      return false;
    }
    yellow_taxi_trips that = (yellow_taxi_trips) o;
    boolean equal = true;
    equal = equal && (this.trip_id == null ? that.trip_id == null : this.trip_id.equals(that.trip_id));
    equal = equal && (this.vendorid == null ? that.vendorid == null : this.vendorid.equals(that.vendorid));
    equal = equal && (this.tpep_pickup_datetime == null ? that.tpep_pickup_datetime == null : this.tpep_pickup_datetime.equals(that.tpep_pickup_datetime));
    equal = equal && (this.tpep_dropoff_datetime == null ? that.tpep_dropoff_datetime == null : this.tpep_dropoff_datetime.equals(that.tpep_dropoff_datetime));
    equal = equal && (this.passenger_count == null ? that.passenger_count == null : this.passenger_count.equals(that.passenger_count));
    equal = equal && (this.trip_distance == null ? that.trip_distance == null : this.trip_distance.equals(that.trip_distance));
    equal = equal && (this.ratecodeid == null ? that.ratecodeid == null : this.ratecodeid.equals(that.ratecodeid));
    equal = equal && (this.store_and_fwd_flag == null ? that.store_and_fwd_flag == null : this.store_and_fwd_flag.equals(that.store_and_fwd_flag));
    equal = equal && (this.pulocationid == null ? that.pulocationid == null : this.pulocationid.equals(that.pulocationid));
    equal = equal && (this.dolocationid == null ? that.dolocationid == null : this.dolocationid.equals(that.dolocationid));
    equal = equal && (this.payment_type == null ? that.payment_type == null : this.payment_type.equals(that.payment_type));
    equal = equal && (this.fare_amount == null ? that.fare_amount == null : this.fare_amount.equals(that.fare_amount));
    equal = equal && (this.extra == null ? that.extra == null : this.extra.equals(that.extra));
    equal = equal && (this.mta_tax == null ? that.mta_tax == null : this.mta_tax.equals(that.mta_tax));
    equal = equal && (this.tip_amount == null ? that.tip_amount == null : this.tip_amount.equals(that.tip_amount));
    equal = equal && (this.tolls_amount == null ? that.tolls_amount == null : this.tolls_amount.equals(that.tolls_amount));
    equal = equal && (this.improvement_surcharge == null ? that.improvement_surcharge == null : this.improvement_surcharge.equals(that.improvement_surcharge));
    equal = equal && (this.total_amount == null ? that.total_amount == null : this.total_amount.equals(that.total_amount));
    equal = equal && (this.congestion_surcharge == null ? that.congestion_surcharge == null : this.congestion_surcharge.equals(that.congestion_surcharge));
    equal = equal && (this.airport_fee == null ? that.airport_fee == null : this.airport_fee.equals(that.airport_fee));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.trip_id = JdbcWritableBridge.readLong(1, __dbResults);
    this.vendorid = JdbcWritableBridge.readInteger(2, __dbResults);
    this.tpep_pickup_datetime = JdbcWritableBridge.readTimestamp(3, __dbResults);
    this.tpep_dropoff_datetime = JdbcWritableBridge.readTimestamp(4, __dbResults);
    this.passenger_count = JdbcWritableBridge.readDouble(5, __dbResults);
    this.trip_distance = JdbcWritableBridge.readDouble(6, __dbResults);
    this.ratecodeid = JdbcWritableBridge.readDouble(7, __dbResults);
    this.store_and_fwd_flag = JdbcWritableBridge.readString(8, __dbResults);
    this.pulocationid = JdbcWritableBridge.readInteger(9, __dbResults);
    this.dolocationid = JdbcWritableBridge.readInteger(10, __dbResults);
    this.payment_type = JdbcWritableBridge.readDouble(11, __dbResults);
    this.fare_amount = JdbcWritableBridge.readDouble(12, __dbResults);
    this.extra = JdbcWritableBridge.readDouble(13, __dbResults);
    this.mta_tax = JdbcWritableBridge.readDouble(14, __dbResults);
    this.tip_amount = JdbcWritableBridge.readDouble(15, __dbResults);
    this.tolls_amount = JdbcWritableBridge.readDouble(16, __dbResults);
    this.improvement_surcharge = JdbcWritableBridge.readDouble(17, __dbResults);
    this.total_amount = JdbcWritableBridge.readDouble(18, __dbResults);
    this.congestion_surcharge = JdbcWritableBridge.readDouble(19, __dbResults);
    this.airport_fee = JdbcWritableBridge.readDouble(20, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.trip_id = JdbcWritableBridge.readLong(1, __dbResults);
    this.vendorid = JdbcWritableBridge.readInteger(2, __dbResults);
    this.tpep_pickup_datetime = JdbcWritableBridge.readTimestamp(3, __dbResults);
    this.tpep_dropoff_datetime = JdbcWritableBridge.readTimestamp(4, __dbResults);
    this.passenger_count = JdbcWritableBridge.readDouble(5, __dbResults);
    this.trip_distance = JdbcWritableBridge.readDouble(6, __dbResults);
    this.ratecodeid = JdbcWritableBridge.readDouble(7, __dbResults);
    this.store_and_fwd_flag = JdbcWritableBridge.readString(8, __dbResults);
    this.pulocationid = JdbcWritableBridge.readInteger(9, __dbResults);
    this.dolocationid = JdbcWritableBridge.readInteger(10, __dbResults);
    this.payment_type = JdbcWritableBridge.readDouble(11, __dbResults);
    this.fare_amount = JdbcWritableBridge.readDouble(12, __dbResults);
    this.extra = JdbcWritableBridge.readDouble(13, __dbResults);
    this.mta_tax = JdbcWritableBridge.readDouble(14, __dbResults);
    this.tip_amount = JdbcWritableBridge.readDouble(15, __dbResults);
    this.tolls_amount = JdbcWritableBridge.readDouble(16, __dbResults);
    this.improvement_surcharge = JdbcWritableBridge.readDouble(17, __dbResults);
    this.total_amount = JdbcWritableBridge.readDouble(18, __dbResults);
    this.congestion_surcharge = JdbcWritableBridge.readDouble(19, __dbResults);
    this.airport_fee = JdbcWritableBridge.readDouble(20, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(trip_id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeInteger(vendorid, 2 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeTimestamp(tpep_pickup_datetime, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(tpep_dropoff_datetime, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeDouble(passenger_count, 5 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(trip_distance, 6 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(ratecodeid, 7 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeString(store_and_fwd_flag, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(pulocationid, 9 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(dolocationid, 10 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDouble(payment_type, 11 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(fare_amount, 12 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(extra, 13 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(mta_tax, 14 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(tip_amount, 15 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(tolls_amount, 16 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(improvement_surcharge, 17 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(total_amount, 18 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(congestion_surcharge, 19 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(airport_fee, 20 + __off, 8, __dbStmt);
    return 20;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(trip_id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeInteger(vendorid, 2 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeTimestamp(tpep_pickup_datetime, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(tpep_dropoff_datetime, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeDouble(passenger_count, 5 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(trip_distance, 6 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(ratecodeid, 7 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeString(store_and_fwd_flag, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(pulocationid, 9 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(dolocationid, 10 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDouble(payment_type, 11 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(fare_amount, 12 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(extra, 13 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(mta_tax, 14 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(tip_amount, 15 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(tolls_amount, 16 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(improvement_surcharge, 17 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(total_amount, 18 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(congestion_surcharge, 19 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(airport_fee, 20 + __off, 8, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.trip_id = null;
    } else {
    this.trip_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.vendorid = null;
    } else {
    this.vendorid = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.tpep_pickup_datetime = null;
    } else {
    this.tpep_pickup_datetime = new Timestamp(__dataIn.readLong());
    this.tpep_pickup_datetime.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.tpep_dropoff_datetime = null;
    } else {
    this.tpep_dropoff_datetime = new Timestamp(__dataIn.readLong());
    this.tpep_dropoff_datetime.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.passenger_count = null;
    } else {
    this.passenger_count = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.trip_distance = null;
    } else {
    this.trip_distance = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.ratecodeid = null;
    } else {
    this.ratecodeid = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.store_and_fwd_flag = null;
    } else {
    this.store_and_fwd_flag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.pulocationid = null;
    } else {
    this.pulocationid = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.dolocationid = null;
    } else {
    this.dolocationid = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.payment_type = null;
    } else {
    this.payment_type = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.fare_amount = null;
    } else {
    this.fare_amount = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.extra = null;
    } else {
    this.extra = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.mta_tax = null;
    } else {
    this.mta_tax = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.tip_amount = null;
    } else {
    this.tip_amount = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.tolls_amount = null;
    } else {
    this.tolls_amount = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.improvement_surcharge = null;
    } else {
    this.improvement_surcharge = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.total_amount = null;
    } else {
    this.total_amount = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.congestion_surcharge = null;
    } else {
    this.congestion_surcharge = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.airport_fee = null;
    } else {
    this.airport_fee = Double.valueOf(__dataIn.readDouble());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.trip_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.trip_id);
    }
    if (null == this.vendorid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.vendorid);
    }
    if (null == this.tpep_pickup_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.tpep_pickup_datetime.getTime());
    __dataOut.writeInt(this.tpep_pickup_datetime.getNanos());
    }
    if (null == this.tpep_dropoff_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.tpep_dropoff_datetime.getTime());
    __dataOut.writeInt(this.tpep_dropoff_datetime.getNanos());
    }
    if (null == this.passenger_count) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.passenger_count);
    }
    if (null == this.trip_distance) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.trip_distance);
    }
    if (null == this.ratecodeid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.ratecodeid);
    }
    if (null == this.store_and_fwd_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, store_and_fwd_flag);
    }
    if (null == this.pulocationid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.pulocationid);
    }
    if (null == this.dolocationid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.dolocationid);
    }
    if (null == this.payment_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.payment_type);
    }
    if (null == this.fare_amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.fare_amount);
    }
    if (null == this.extra) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.extra);
    }
    if (null == this.mta_tax) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.mta_tax);
    }
    if (null == this.tip_amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.tip_amount);
    }
    if (null == this.tolls_amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.tolls_amount);
    }
    if (null == this.improvement_surcharge) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.improvement_surcharge);
    }
    if (null == this.total_amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.total_amount);
    }
    if (null == this.congestion_surcharge) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.congestion_surcharge);
    }
    if (null == this.airport_fee) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.airport_fee);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.trip_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.trip_id);
    }
    if (null == this.vendorid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.vendorid);
    }
    if (null == this.tpep_pickup_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.tpep_pickup_datetime.getTime());
    __dataOut.writeInt(this.tpep_pickup_datetime.getNanos());
    }
    if (null == this.tpep_dropoff_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.tpep_dropoff_datetime.getTime());
    __dataOut.writeInt(this.tpep_dropoff_datetime.getNanos());
    }
    if (null == this.passenger_count) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.passenger_count);
    }
    if (null == this.trip_distance) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.trip_distance);
    }
    if (null == this.ratecodeid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.ratecodeid);
    }
    if (null == this.store_and_fwd_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, store_and_fwd_flag);
    }
    if (null == this.pulocationid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.pulocationid);
    }
    if (null == this.dolocationid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.dolocationid);
    }
    if (null == this.payment_type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.payment_type);
    }
    if (null == this.fare_amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.fare_amount);
    }
    if (null == this.extra) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.extra);
    }
    if (null == this.mta_tax) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.mta_tax);
    }
    if (null == this.tip_amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.tip_amount);
    }
    if (null == this.tolls_amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.tolls_amount);
    }
    if (null == this.improvement_surcharge) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.improvement_surcharge);
    }
    if (null == this.total_amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.total_amount);
    }
    if (null == this.congestion_surcharge) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.congestion_surcharge);
    }
    if (null == this.airport_fee) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.airport_fee);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(trip_id==null?"null":"" + trip_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(vendorid==null?"null":"" + vendorid, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tpep_pickup_datetime==null?"null":"" + tpep_pickup_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tpep_dropoff_datetime==null?"null":"" + tpep_dropoff_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(passenger_count==null?"null":"" + passenger_count, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(trip_distance==null?"null":"" + trip_distance, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ratecodeid==null?"null":"" + ratecodeid, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(store_and_fwd_flag==null?"null":store_and_fwd_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pulocationid==null?"null":"" + pulocationid, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dolocationid==null?"null":"" + dolocationid, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(payment_type==null?"null":"" + payment_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(fare_amount==null?"null":"" + fare_amount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(extra==null?"null":"" + extra, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(mta_tax==null?"null":"" + mta_tax, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tip_amount==null?"null":"" + tip_amount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tolls_amount==null?"null":"" + tolls_amount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(improvement_surcharge==null?"null":"" + improvement_surcharge, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(total_amount==null?"null":"" + total_amount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(congestion_surcharge==null?"null":"" + congestion_surcharge, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(airport_fee==null?"null":"" + airport_fee, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(trip_id==null?"null":"" + trip_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(vendorid==null?"null":"" + vendorid, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tpep_pickup_datetime==null?"null":"" + tpep_pickup_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tpep_dropoff_datetime==null?"null":"" + tpep_dropoff_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(passenger_count==null?"null":"" + passenger_count, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(trip_distance==null?"null":"" + trip_distance, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ratecodeid==null?"null":"" + ratecodeid, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(store_and_fwd_flag==null?"null":store_and_fwd_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pulocationid==null?"null":"" + pulocationid, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dolocationid==null?"null":"" + dolocationid, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(payment_type==null?"null":"" + payment_type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(fare_amount==null?"null":"" + fare_amount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(extra==null?"null":"" + extra, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(mta_tax==null?"null":"" + mta_tax, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tip_amount==null?"null":"" + tip_amount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tolls_amount==null?"null":"" + tolls_amount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(improvement_surcharge==null?"null":"" + improvement_surcharge, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(total_amount==null?"null":"" + total_amount, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(congestion_surcharge==null?"null":"" + congestion_surcharge, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(airport_fee==null?"null":"" + airport_fee, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.trip_id = null; } else {
      this.trip_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.vendorid = null; } else {
      this.vendorid = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tpep_pickup_datetime = null; } else {
      this.tpep_pickup_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tpep_dropoff_datetime = null; } else {
      this.tpep_dropoff_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.passenger_count = null; } else {
      this.passenger_count = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.trip_distance = null; } else {
      this.trip_distance = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ratecodeid = null; } else {
      this.ratecodeid = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.store_and_fwd_flag = null; } else {
      this.store_and_fwd_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.pulocationid = null; } else {
      this.pulocationid = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dolocationid = null; } else {
      this.dolocationid = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.payment_type = null; } else {
      this.payment_type = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.fare_amount = null; } else {
      this.fare_amount = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.extra = null; } else {
      this.extra = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.mta_tax = null; } else {
      this.mta_tax = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tip_amount = null; } else {
      this.tip_amount = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tolls_amount = null; } else {
      this.tolls_amount = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.improvement_surcharge = null; } else {
      this.improvement_surcharge = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.total_amount = null; } else {
      this.total_amount = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.congestion_surcharge = null; } else {
      this.congestion_surcharge = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.airport_fee = null; } else {
      this.airport_fee = Double.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.trip_id = null; } else {
      this.trip_id = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.vendorid = null; } else {
      this.vendorid = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tpep_pickup_datetime = null; } else {
      this.tpep_pickup_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tpep_dropoff_datetime = null; } else {
      this.tpep_dropoff_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.passenger_count = null; } else {
      this.passenger_count = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.trip_distance = null; } else {
      this.trip_distance = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ratecodeid = null; } else {
      this.ratecodeid = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.store_and_fwd_flag = null; } else {
      this.store_and_fwd_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.pulocationid = null; } else {
      this.pulocationid = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dolocationid = null; } else {
      this.dolocationid = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.payment_type = null; } else {
      this.payment_type = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.fare_amount = null; } else {
      this.fare_amount = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.extra = null; } else {
      this.extra = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.mta_tax = null; } else {
      this.mta_tax = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tip_amount = null; } else {
      this.tip_amount = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tolls_amount = null; } else {
      this.tolls_amount = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.improvement_surcharge = null; } else {
      this.improvement_surcharge = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.total_amount = null; } else {
      this.total_amount = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.congestion_surcharge = null; } else {
      this.congestion_surcharge = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.airport_fee = null; } else {
      this.airport_fee = Double.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    yellow_taxi_trips o = (yellow_taxi_trips) super.clone();
    o.tpep_pickup_datetime = (o.tpep_pickup_datetime != null) ? (java.sql.Timestamp) o.tpep_pickup_datetime.clone() : null;
    o.tpep_dropoff_datetime = (o.tpep_dropoff_datetime != null) ? (java.sql.Timestamp) o.tpep_dropoff_datetime.clone() : null;
    return o;
  }

  public void clone0(yellow_taxi_trips o) throws CloneNotSupportedException {
    o.tpep_pickup_datetime = (o.tpep_pickup_datetime != null) ? (java.sql.Timestamp) o.tpep_pickup_datetime.clone() : null;
    o.tpep_dropoff_datetime = (o.tpep_dropoff_datetime != null) ? (java.sql.Timestamp) o.tpep_dropoff_datetime.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("trip_id", this.trip_id);
    __sqoop$field_map.put("vendorid", this.vendorid);
    __sqoop$field_map.put("tpep_pickup_datetime", this.tpep_pickup_datetime);
    __sqoop$field_map.put("tpep_dropoff_datetime", this.tpep_dropoff_datetime);
    __sqoop$field_map.put("passenger_count", this.passenger_count);
    __sqoop$field_map.put("trip_distance", this.trip_distance);
    __sqoop$field_map.put("ratecodeid", this.ratecodeid);
    __sqoop$field_map.put("store_and_fwd_flag", this.store_and_fwd_flag);
    __sqoop$field_map.put("pulocationid", this.pulocationid);
    __sqoop$field_map.put("dolocationid", this.dolocationid);
    __sqoop$field_map.put("payment_type", this.payment_type);
    __sqoop$field_map.put("fare_amount", this.fare_amount);
    __sqoop$field_map.put("extra", this.extra);
    __sqoop$field_map.put("mta_tax", this.mta_tax);
    __sqoop$field_map.put("tip_amount", this.tip_amount);
    __sqoop$field_map.put("tolls_amount", this.tolls_amount);
    __sqoop$field_map.put("improvement_surcharge", this.improvement_surcharge);
    __sqoop$field_map.put("total_amount", this.total_amount);
    __sqoop$field_map.put("congestion_surcharge", this.congestion_surcharge);
    __sqoop$field_map.put("airport_fee", this.airport_fee);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("trip_id", this.trip_id);
    __sqoop$field_map.put("vendorid", this.vendorid);
    __sqoop$field_map.put("tpep_pickup_datetime", this.tpep_pickup_datetime);
    __sqoop$field_map.put("tpep_dropoff_datetime", this.tpep_dropoff_datetime);
    __sqoop$field_map.put("passenger_count", this.passenger_count);
    __sqoop$field_map.put("trip_distance", this.trip_distance);
    __sqoop$field_map.put("ratecodeid", this.ratecodeid);
    __sqoop$field_map.put("store_and_fwd_flag", this.store_and_fwd_flag);
    __sqoop$field_map.put("pulocationid", this.pulocationid);
    __sqoop$field_map.put("dolocationid", this.dolocationid);
    __sqoop$field_map.put("payment_type", this.payment_type);
    __sqoop$field_map.put("fare_amount", this.fare_amount);
    __sqoop$field_map.put("extra", this.extra);
    __sqoop$field_map.put("mta_tax", this.mta_tax);
    __sqoop$field_map.put("tip_amount", this.tip_amount);
    __sqoop$field_map.put("tolls_amount", this.tolls_amount);
    __sqoop$field_map.put("improvement_surcharge", this.improvement_surcharge);
    __sqoop$field_map.put("total_amount", this.total_amount);
    __sqoop$field_map.put("congestion_surcharge", this.congestion_surcharge);
    __sqoop$field_map.put("airport_fee", this.airport_fee);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
