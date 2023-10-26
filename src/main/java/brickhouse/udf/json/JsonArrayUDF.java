package brickhouse.udf.json;

import brickhouse.udf.json.InspectorHandle.InspectorHandleFactory;
import java.io.IOException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

@Description(name = "json_array",
    value = "_FUNC_(json) - Returns a list of elements from a JSON array string"
)
public class JsonArrayUDF extends GenericUDF {
  private StringObjectInspector stringInspector;
  private InspectorHandle inspHandle;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("Usage : json_array( jsonstring ) ");
    }
    if (!arguments[0].getCategory().equals(Category.PRIMITIVE)
        || ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveCategory.STRING) {
      throw new UDFArgumentException("Usage : json_array( jsonstring ) ");
    }

    stringInspector = (StringObjectInspector) arguments[0];

    ObjectInspector elemInsp = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

    ListObjectInspector listInsp = ObjectInspectorFactory.getStandardListObjectInspector(elemInsp);

    inspHandle = InspectorHandleFactory.GenerateInspectorHandle(listInsp);

    return inspHandle.getReturnType();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    try {
      String jsonString = this.stringInspector.getPrimitiveJavaObject(arguments[0].get());

      ObjectMapper om = new ObjectMapper();
      JsonNode jsonNode = om.readTree(jsonString);
      return inspHandle.parseJson(jsonNode);

    } catch (JsonProcessingException e) {
      throw new HiveException(e);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "json_array( " + children[0] + " )";
  }
}

