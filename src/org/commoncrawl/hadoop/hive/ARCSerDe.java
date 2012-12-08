package org.commoncrawl.hadoop.hive;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.Writable;

import static org.apache.hadoop.hive.serde.Constants.*;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.*;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import org.commoncrawl.protocol.shared.*;
import org.commoncrawl.util.shared.FlexBuffer;
import org.commoncrawl.util.shared.ImmutableBuffer;
import org.commoncrawl.util.shared.TextBytes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ARCSerDe implements SerDe {

	private ObjectInspector objectInspector;
	private List<Object> row;

	/*
	 * 
	 * Fields:
	 * 
	 * uri: String hostIP: String timestamp: Long mimeType: String recordLength:
	 * Int headerItems: List[ArcFileHeaderItem] content: FlexBuffer (?)
	 * arcFileName: String arcFilePos: Int flags: Int arcFileSize: Int
	 */

	private static final String URI = "uri";
	private static final String HOST_IP = "hostIP";
	private static final String TIMESTAMP = "timestamp";
	private static final String MIME_TYPE = "mimeType";
	private static final String RECORD_LENGTH = "recordLength";
	private static final String FILE_HEADERS = "fileHeaders";
	private static final String CONTENT = "content";
	private static final String ARC_FILE_NAME = "arcFileName";
	private static final String ARC_FILE_POS = "arcFilePos";
	private static final String FLAGS = "flags";
	private static final String ARC_FILE_SIZE = "fileSize";

	private static final String HEADER_KEY = "key";
	private static final String HEADER_VALUE = "value";

	private void verifyColumnType(List<TypeInfo> actualTypes, int index,
			TypeInfo expected) throws SerDeException {
		if (actualTypes.get(index) != expected) {
			throw new SerDeException("Column " + index + " must be type "
					+ expected.getTypeName());
		}
	}

	@Override
	public void initialize(Configuration config, Properties table)
			throws SerDeException {
		String columnTypeProperty = table.getProperty(LIST_COLUMN_TYPES);
		List<TypeInfo> columnTypes = TypeInfoUtils
				.getTypeInfosFromTypeString(columnTypeProperty);

		if (columnTypes.size() != 11) {
			throw new SerDeException("Table must have 11 columns.");
		}

		TypeInfo headerTypeInfo = TypeInfoFactory
				.getListTypeInfo(TypeInfoFactory.getStructTypeInfo(
						ImmutableList.of(HEADER_KEY, HEADER_VALUE),
						ImmutableList.of(TypeInfoFactory.stringTypeInfo,
								TypeInfoFactory.stringTypeInfo)));

		verifyColumnType(columnTypes, 0, TypeInfoFactory.stringTypeInfo);
		verifyColumnType(columnTypes, 1, TypeInfoFactory.stringTypeInfo);
		verifyColumnType(columnTypes, 2, TypeInfoFactory.longTypeInfo);
		verifyColumnType(columnTypes, 3, TypeInfoFactory.stringTypeInfo);
		verifyColumnType(columnTypes, 4, TypeInfoFactory.intTypeInfo);
		verifyColumnType(columnTypes, 5, headerTypeInfo);
		verifyColumnType(columnTypes, 6, TypeInfoFactory.stringTypeInfo);
		verifyColumnType(columnTypes, 7, TypeInfoFactory.stringTypeInfo);
		verifyColumnType(columnTypes, 8, TypeInfoFactory.intTypeInfo);
		verifyColumnType(columnTypes, 9, TypeInfoFactory.intTypeInfo);
		verifyColumnType(columnTypes, 10, TypeInfoFactory.intTypeInfo);

		// ARC File Header Item Object Inspector
		List<String> fileHeaderFieldNames = ImmutableList.of(HEADER_KEY,
				HEADER_VALUE);
		List<ObjectInspector> fileHeaderFieldOIs = ImmutableList
				.<ObjectInspector> of(javaStringObjectInspector,
						javaStringObjectInspector);
		StructObjectInspector fileHeaderOI = ObjectInspectorFactory
				.getStandardStructObjectInspector(fileHeaderFieldNames,
						fileHeaderFieldOIs);

		List<String> fieldNames = ImmutableList.of(URI, HOST_IP, TIMESTAMP,
				MIME_TYPE, RECORD_LENGTH, FILE_HEADERS, CONTENT, ARC_FILE_NAME,
				ARC_FILE_POS, FLAGS, ARC_FILE_SIZE);
		List<ObjectInspector> fieldOIs = ImmutableList.<ObjectInspector> of(
				javaStringObjectInspector, // uri
				javaStringObjectInspector, // hostIP
				javaLongObjectInspector, // timestamp
				javaStringObjectInspector, // mimeType
				javaIntObjectInspector, // recordLength
				ObjectInspectorFactory
						.getStandardListObjectInspector(fileHeaderOI), // ArcFileHeaderItem
				javaStringObjectInspector, // content
				javaStringObjectInspector, // arcFileName
				javaIntObjectInspector, // arcFilePos
				javaIntObjectInspector, // flags
				javaIntObjectInspector // file size
				);
		objectInspector = ObjectInspectorFactory
				.getStandardStructObjectInspector(fieldNames, fieldOIs);

		row = Lists.newArrayListWithCapacity(fieldOIs.size());
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return objectInspector;
	}

	/** Does not support SerDeStats. */
	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return ArcFileItem.class;
	}

	@Override
	public Object deserialize(Writable writable) throws SerDeException {
		if (writable == null) {
			return null;
		}
		if (!(writable instanceof ArcFileItem)) {
			throw new SerDeException("Expected an ArcFileItem, received a "
					+ writable.getClass());
		}
		ArcFileItem arcFileItem = (ArcFileItem) writable;

		row.clear();

		row.set(0, arcFileItem.getUri());
		row.set(1, arcFileItem.getHostIP());
		row.set(2, arcFileItem.getTimestamp());
		row.set(3, arcFileItem.getMimeType());
		row.set(4, arcFileItem.getRecordLength());

		if (arcFileItem.getHeaderItems() == null) {
			row.set(5, Collections.emptyList());
		} else {
			List<List<String>> headers = Lists
					.newArrayListWithCapacity(arcFileItem.getHeaderItems()
							.size());
			for (ArcFileHeaderItem item : arcFileItem.getHeaderItems()) {
				List<String> header = Lists.newArrayListWithCapacity(2);
				header.add(item.getItemKey());
				header.add(item.getItemValue());
				headers.add(header);
			}
			row.set(5, headers);
		}

		row.set(6, new String(arcFileItem.getContent().getReadOnlyBytes()));
		row.set(7, arcFileItem.getArcFileName());
		row.set(8, arcFileItem.getArcFilePos());
		row.set(9, arcFileItem.getFlags());
		row.set(10, arcFileItem.getArcFileSize());

		return row;
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objectInspector)
			throws SerDeException {

		if (!(objectInspector instanceof StructObjectInspector)) {
			throw new SerDeException();
		}

		StructObjectInspector structOI = (StructObjectInspector) objectInspector;
		List<? extends StructField> fieldRefs = structOI
				.getAllStructFieldRefs();

		Map<String, StructField> fieldRefMap = Maps
				.newHashMapWithExpectedSize(fieldRefs.size());
		for (StructField fieldRef : fieldRefs) {
			fieldRefMap.put(fieldRef.getFieldName(), fieldRef);
		}

		ArcFileItem arcFileItem = new ArcFileItem();

		arcFileItem.setUri(inspectString(obj, structOI, fieldRefMap.get(URI)));
		arcFileItem.setHostIP(inspectString(obj, structOI,
				fieldRefMap.get(HOST_IP)));
		arcFileItem.setTimestamp(inspectLong(obj, structOI,
				fieldRefMap.get(TIMESTAMP)));
		arcFileItem.setMimeType(inspectString(obj, structOI,
				fieldRefMap.get(MIME_TYPE)));
		arcFileItem.setRecordLength(inspectInt(obj, structOI,
				fieldRefMap.get(RECORD_LENGTH)));
		arcFileItem.setHeaderItems(inspectHeaders(obj, structOI,
				fieldRefMap.get(FILE_HEADERS)));
		arcFileItem.setContent(new FlexBuffer(inspectString(obj, structOI,
				fieldRefMap.get(CONTENT)).getBytes(), false));
		arcFileItem.setArcFileName(inspectString(obj, structOI,
				fieldRefMap.get(ARC_FILE_NAME)));
		arcFileItem.setArcFilePos(inspectInt(obj, structOI,
				fieldRefMap.get(ARC_FILE_POS)));
		arcFileItem.setFlags(inspectInt(obj, structOI, fieldRefMap.get(FLAGS)));
		arcFileItem.setArcFileSize(inspectInt(obj, structOI,
				fieldRefMap.get(ARC_FILE_SIZE)));

		return arcFileItem;
	}

	private String inspectString(Object obj,
			StructObjectInspector objectInspector, StructField field)
			throws SerDeException {
		if (field.getFieldObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE
				|| ((PrimitiveObjectInspector) field.getFieldObjectInspector())
						.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new SerDeException(String.format(
					"Field %s is not a String field.", field.getFieldName()));
		}
		Object fieldData = objectInspector.getStructFieldData(obj, field);
		return ((StringObjectInspector) field.getFieldObjectInspector())
				.getPrimitiveJavaObject(fieldData);
	}

	private Long inspectLong(Object obj, StructObjectInspector objectInspector,
			StructField field) throws SerDeException {
		if (field.getFieldObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE
				|| ((PrimitiveObjectInspector) field.getFieldObjectInspector())
						.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG) {
			throw new SerDeException(String.format(
					"Field %s is not a Long field.", field.getFieldName()));
		}
		Object fieldData = objectInspector.getStructFieldData(obj, field);
		return (Long) ((LongObjectInspector) field.getFieldObjectInspector())
				.getPrimitiveJavaObject(fieldData);
	}

	private Integer inspectInt(Object obj,
			StructObjectInspector objectInspector, StructField field)
			throws SerDeException {
		if (field.getFieldObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE
				|| ((PrimitiveObjectInspector) field.getFieldObjectInspector())
						.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
			throw new SerDeException(String.format(
					"Field %s is not an Int field.", field.getFieldName()));
		}
		Object fieldData = objectInspector.getStructFieldData(obj, field);
		return (Integer) ((IntObjectInspector) field.getFieldObjectInspector())
				.getPrimitiveJavaObject(fieldData);
	}

	private ArrayList<ArcFileHeaderItem> inspectHeaders(Object obj,
			StructObjectInspector objectInspector, StructField field)
			throws SerDeException {
		if (field.getFieldObjectInspector().getCategory() != ObjectInspector.Category.LIST) {
			throw new SerDeException(String.format(
					"Field %s is not a List field.", field.getFieldName()));
		}
		ListObjectInspector listInspector = ((ListObjectInspector) field
				.getFieldObjectInspector());

		ObjectInspector elementInspector = listInspector
				.getListElementObjectInspector();
		if (elementInspector.getCategory() != ObjectInspector.Category.STRUCT) {
			throw new SerDeException(String.format(
					"Field %s does not contain Structs.", field.getFieldName()));
		}
		StructObjectInspector headerInspector = (StructObjectInspector) elementInspector;
		StructField keyField = headerInspector.getAllStructFieldRefs().get(0);
		StructField valueField = headerInspector.getAllStructFieldRefs().get(1);

		Object listObj = objectInspector.getStructFieldData(obj, field);
		final int listLength = listInspector.getListLength(listObj);
		ArrayList<ArcFileHeaderItem> headerItems = Lists
				.newArrayListWithCapacity(listLength);
		for (int i = 0; i < listLength; i++) {
			Object elemObj = listInspector.getListElement(listObj, i);
			ArcFileHeaderItem headerItem = new ArcFileHeaderItem();
			headerItem.setItemKey(inspectString(elemObj, headerInspector,
					keyField));
			headerItem.setItemValue(inspectString(elemObj, headerInspector,
					valueField));
			headerItems.add(headerItem);
		}
		return headerItems;
	}

}
