/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.hive.utils;

import static org.kiji.hive.utils.HiveTypes.HiveList;
import static org.kiji.hive.utils.HiveTypes.HiveMap;
import static org.kiji.hive.utils.HiveTypes.HiveStruct;
import static org.kiji.hive.utils.HiveTypes.HiveUnion;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.ql.exec.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Converts an Avro data object to an in-memory representation for Hive.
 *
 * <p>This prepares hive objects under the assumption that we are
 * using the "standard" object inspectors. See the classes in
 * org.apache.hadoop.hive.serde2.objectinspector that start with
 * "Standard" for details about how each Hive type should be formatted.</p>
 */
public final class AvroTypeAdapter {
  /** Private constructor to prevent instantiation. Use get() to get an instance of this. */
  private AvroTypeAdapter() {}

  /** Singleton instance. */
  private static final AvroTypeAdapter SINGLETON = new AvroTypeAdapter();

  /** @return an instance of AvroTypeAdapter. */
  public static AvroTypeAdapter get() {
    return SINGLETON;
  }

  /**
   * Indicates that an Avro data type is not compatible with a Hive type.
   */
  public static class IncompatibleTypeException extends RuntimeException {
    /**
     * Constructor.
     *
     * @param hiveType The hive type.
     * @param avroData The avro data.
     */
    public IncompatibleTypeException(TypeInfo hiveType, Object avroData) {
      super("Unable to convert avro data [" + avroData + "] to hive type [" + hiveType + "]");
    }

    /**
     * Constructor.
     *
     * @param hiveType The hive type.
     */
    public IncompatibleTypeException(TypeInfo hiveType) {
      super("Unable to generate an avro schema that describes hive type [" + hiveType + "]");
    }
  }

  /**
   * Converts a piece avro data to a hive in-memory object.  This method will recursively
   * unpack the objects for any non-primitive types.
   *
   * @param hiveType The type of the target hive object.
   * @param avro The avro data to convert.
   * @param schema The schema the passed in type.
   * @return The converted hive datum, compatible with the standard object inspector.
   */
  public Object toHiveType(TypeInfo hiveType, Object avro, Schema schema) {
    if (null == avro) {
      return null;
    }

    switch (hiveType.getCategory()) {
    case PRIMITIVE:
      return toHiveType((PrimitiveTypeInfo) hiveType, avro);
    case LIST:
      HiveList<Object> hiveList = new HiveList<Object>();
      @SuppressWarnings("unchecked")
      final List<Object> avroList = (List<Object>) avro;
      final TypeInfo listElementType = ((ListTypeInfo) hiveType).getListElementTypeInfo();
      for (Object avroElement : avroList) {
        hiveList.add(toHiveType(listElementType, avroElement, schema.getElementType()));
      }
      return hiveList;
    case MAP:
      HiveMap<String, Object> hiveMap = new HiveMap<String, Object>();
      @SuppressWarnings("unchecked")
      final Map<CharSequence, Object> avroMap = (Map<CharSequence, Object>) avro;
      final TypeInfo mapValueType = ((MapTypeInfo) hiveType).getMapValueTypeInfo();
      for (Map.Entry<CharSequence, Object> avroEntry : avroMap.entrySet()) {
        String entryKey = avroEntry.getKey().toString();
        Object entryValue = toHiveType(mapValueType, avroEntry.getValue(), schema.getValueType());
        hiveMap.put(entryKey, entryValue);
      }
      return hiveMap;
    case STRUCT:
      HiveStruct hiveStruct = new HiveStruct();
      final GenericRecord avroRecord = (GenericRecord) avro;
      final StructTypeInfo hiveStructType = (StructTypeInfo) hiveType;
      List<Schema> schemaList = Lists.newArrayList();
      for (Schema.Field field : schema.getFields()) {
        schemaList.add(field.schema());
      }
      for (int i = 0; i < hiveStructType.getAllStructFieldNames().size(); i++) {
        final String fieldName = hiveStructType.getAllStructFieldNames().get(i);
        final TypeInfo fieldType = hiveStructType.getAllStructFieldTypeInfos().get(i);
        hiveStruct.add(toHiveType(fieldType, avroRecord.get(fieldName), schemaList.get(i)));
      }
      return hiveStruct;
    case UNION:
      HiveUnion hiveUnion = new HiveUnion();
      final Integer tag = GenericData.get().resolveUnion(schema, avro);
      hiveUnion.setTag(tag.byteValue());
      Schema unionSubSchema = schema.getTypes().get(tag);

      final UnionTypeInfo hiveUnionType = (UnionTypeInfo) hiveType;
      final TypeInfo unionSubType = hiveUnionType.getAllUnionObjectTypeInfos().get(tag);
      hiveUnion.setObject(toHiveType(unionSubType, avro, unionSubSchema));
      return hiveUnion;
    default:
      throw new IncompatibleTypeException(hiveType, avro);
    }
  }

  /**
   * Converts data from Avro into a Hive primitive type.
   *
   * @param primitiveType The target Hive type.
   * @param avro The avro datum.
   * @return The converted Hive object.
   */
  public Object toHiveType(PrimitiveTypeInfo primitiveType, Object avro) {
    switch (primitiveType.getPrimitiveCategory()) {

    case VOID: // Like the avro null type, right?
      return null;

    case BYTE:
      if (!(avro instanceof GenericFixed)) {
        throw new IncompatibleTypeException(primitiveType, avro);
      }
      return Byte.valueOf(((GenericFixed) avro).bytes()[0]);

    case SHORT:
      if (!(avro instanceof GenericFixed)) {
        throw new IncompatibleTypeException(primitiveType, avro);
      }
      final byte[] fixedBytes = ((GenericFixed) avro).bytes();
      final ByteBuffer bb = ByteBuffer.allocate(2);
      bb.order(ByteOrder.LITTLE_ENDIAN);
      bb.put(fixedBytes[0]);
      bb.put(fixedBytes[1]);
      return bb.getShort(0);

    case BOOLEAN: // These primitive types are the same in avro and hive.
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
      return avro;

    case STRING:
      return avro.toString();

    case TIMESTAMP:
      if (!(avro instanceof Long)) {
        throw new IncompatibleTypeException(primitiveType, avro);
      }
      return new Timestamp(((Long) avro).longValue());

    case BINARY:
      final ByteArrayRef byteArrayRef = new ByteArrayRef();
      final ByteBuffer byteBuffer = (ByteBuffer) avro;
      byteArrayRef.setData(
          Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit()));
      return byteArrayRef;

    default:
      throw new IncompatibleTypeException(primitiveType, avro);
    }
  }

  //FIXME write some classes to convert from these hive objects back to the raw writable types.

  /**
   * Converts data from Hive primitive type into a Writable type that can later be put into a Kiji table.
   *
   * @param primitiveType The target Hive type.
   * @param hiveObject          The hiveObject datum.
   * @return The converted Hive object.
   */
  public Object toWritableType(PrimitiveTypeInfo primitiveType, Object hiveObject) {
    switch (primitiveType.getPrimitiveCategory()) {
      case VOID: // Like the hiveObject null type, right?
        return NullWritable.get();
      case BYTE:
        Byte byteObject = (Byte) hiveObject;
        return new ByteWritable(byteObject);
      case SHORT:
        Short shortObject = (Short) hiveObject;
        return new ShortWritable(shortObject);
      case BOOLEAN:
        Boolean booleanObject = (Boolean) hiveObject;
        return new BooleanWritable(booleanObject);
      case INT:
        Integer intObject = (Integer) hiveObject;
        return new IntWritable(intObject);
      case LONG:
        Long longObject = (Long) hiveObject;
        return new LongWritable(longObject);
      case FLOAT:
        Float floatObject = (Float) hiveObject;
        return new FloatWritable(floatObject);
      case DOUBLE:
        Double doubleObject = (Double) hiveObject;
        return new DoubleWritable(doubleObject);
      case STRING:
        String stringObject = (String) hiveObject;
        return new Text(stringObject);
      case TIMESTAMP:
        Timestamp timestampObject = (Timestamp) hiveObject;
        return new LongWritable(timestampObject.getTime());
      case BINARY:
        //FIXME make sure that this is the right thing to do.
        ByteArrayRef byteArrayRefObject = (ByteArrayRef) hiveObject;
        return new BytesWritable(byteArrayRefObject.getData());
      default:
        throw new IncompatibleTypeException(primitiveType, hiveObject);
    }
  }
}
