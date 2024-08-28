package com.rawtask.xmlrpc;

import java.io.IOException;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import com.rawtask.ByteArray;

public class XMLRpc {
	public static String createResponse(Object obj) throws IOException {
		final Element methodResponse = new Element("methodResponse");
		final Document doc = new Document(methodResponse);

		final Element params = new Element("params");
		final Element param = new Element("param");

		methodResponse.addContent(params);
		params.addContent(param);

		param.addContent(XMLRpc.encodeValue(obj));

		final XMLOutputter xmlOutput = new XMLOutputter();
		xmlOutput.setFormat(Format.getPrettyFormat());
		final StringWriter stringWriter = new StringWriter();
		xmlOutput.output(doc, stringWriter);
		stringWriter.close();

		return stringWriter.toString();
	}

	@SuppressWarnings("unchecked")
	public static Element encodeArray(List<?> list) {
		final Element e = new Element("array");
		final Element d = new Element("data");
		e.addContent(d);
		for (final Object obj : list) {
			final Element v = new Element("value");
			if (obj instanceof String) {
				v.addContent(XMLRpc.encodeString((String) obj));
			} else if (obj instanceof Integer) {
				v.addContent(XMLRpc.encodeInt((Integer) obj));
			} else if (obj instanceof List) {
				v.addContent(XMLRpc.encodeArray((List<Object>) obj));
			} else if (obj instanceof Map) {
				v.addContent(XMLRpc.encodeMap((Map<Object, Object>) obj));
			} else if (obj instanceof Boolean) {
				v.addContent(XMLRpc.encodeBoolean((Boolean) obj));
			} else if (obj instanceof Date) {
				v.addContent(XMLRpc.encodeDate((Date) obj));
			} else {
				throw new IllegalArgumentException("unknown type " + obj);
			}
			d.addContent(v);
		}

		return e;
	}

	private static Element encodeBoolean(Boolean obj) {
		final Element e = new Element("boolean");
		if (obj.booleanValue()) {
			e.setText("1");
		} else {
			e.setText("0");
		}
		return e;
	}

	private static Element encodeByteArray(ByteArray b) {
		final Element eBase64 = new Element("base64");
		eBase64.setText(Base64.encode(b.getBytes()));
		return eBase64;
	}

	private static Element encodeDate(Date obj) {
		final Element e = new Element("dateTime.iso8601");
		final SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd'T'HH:mm:ss");
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		e.setText(df.format(obj));
		return e;
	}

	private static Element encodeInt(Integer obj) {
		final Element e = new Element("int");
		e.setText(obj.toString());
		return e;
	}

	private static Element encodeMap(Map<?, ?> map) {
		final Element eStruct = new Element("struct");
		for (final Iterator<?> iterator = map.keySet().iterator(); iterator.hasNext();) {
			final Element eMember = new Element("member");
			final Object key = iterator.next();
			final Element eName = new Element("name");
			eName.setText(key.toString());
			eMember.addContent(eName);
			final Object value = map.get(key);
			eMember.addContent(XMLRpc.encodeValue(value));
			eStruct.addContent(eMember);
		}
		return eStruct;
	}

	public static Element encodeString(String str) {
		final Element e = new Element("string");
		e.setText(str);
		return e;
	}

	private static Element encodeValue(Object obj) {
		final Element v = new Element("value");
		if (obj == null) {
			v.addContent(XMLRpc.encodeString(""));
		} else {
			if (obj instanceof String) {
				v.addContent(XMLRpc.encodeString((String) obj));
			} else if (obj instanceof Integer) {
				v.addContent(XMLRpc.encodeInt((Integer) obj));
			} else if (obj instanceof List) {
				v.addContent(XMLRpc.encodeArray((List<?>) obj));
			} else if (obj instanceof Map) {
				v.addContent(XMLRpc.encodeMap((Map<?, ?>) obj));
			} else if (obj instanceof Boolean) {
				v.addContent(XMLRpc.encodeBoolean((Boolean) obj));
			} else if (obj instanceof Date) {
				v.addContent(XMLRpc.encodeDate((Date) obj));
			} else if (obj instanceof ByteArray) {
				v.addContent(XMLRpc.encodeByteArray((ByteArray) obj));
			} else {
				throw new IllegalArgumentException("unknown type " + obj);
			}
		}
		return v;
	}

	private static Object getValue(Element value) {
		final List<Element> children = value.getChildren();
		if (children.isEmpty()) {
			return value.getText();
		}
		if (children.size() == 1) {
			final Element type = children.get(0);
			final String name = type.getName();
			if (name.equals("string")) {
				return type.getText();
			} else if (name.equals("int") || name.equals("i4")) {
				return Integer.valueOf(type.getText());
			} else if (name.equals("struct")) {
				final Map<String, Object> map = new HashMap<>();
				final List<Element> eMember = type.getChildren("member");
				for (final Element element : eMember) {
					map.put(element.getChildText("name"), XMLRpc.getValue(element.getChild("value")));
				}
				return map;
			} else if (name.equals("boolean")) {
				if (type.getText().equals("1")) {
					return Boolean.TRUE;
				}
				return Boolean.FALSE;
			} else if (name.equals("dateTime.iso8601")) {
				final SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd'T'HH:mm:ss");
				try {
					return df.parse(type.getText());
				} catch (final ParseException e) {
					return new Date();
				}
			} else if (name.equals("base64")) {
				final String base64 = type.getText();
				final byte[] bytes = Base64.decode(base64);
				return new ByteArray(bytes);
			} else {
				throw new IllegalArgumentException("unknow type " + type);
			}
		}
		final List<Object> result = new ArrayList<>();
		for (final Element element : children) {
			result.add(XMLRpc.getValue(element));
		}
		return result;

	}

	public static List<XMLRpcCall> parse(Element xml) {

		final String MULTICALL = "system.multicall";
		final String methodName = xml.getChildText("methodName");
		if (methodName.equals(MULTICALL)) {
			final List<XMLRpcCall> result = new ArrayList<XMLRpcCall>();
			final Element eData = xml.getChild("params").getChild("param").getChild("value").getChild("array")
					.getChild("data");
			final List<Element> listValue = eData.getChildren("value");
			for (final Element eValue : listValue) {
				final List<Element> listMember = eValue.getChild("struct").getChildren("member");
				String mName = "";
				final List<Object> params = new ArrayList<Object>();
				for (final Element eMember : listMember) {
					final String name = eMember.getChild("name").getText();
					if (name.equals("methodName")) {
						mName = eMember.getChildText("value");
					} else if (name.equals("params")) {
						final Element array = eMember.getChild("value").getChild("array");
						final List<Element> listData = array.getChildren("data");
						for (final Element element : listData) {
							params.add(XMLRpc.getValue(element.getChild("value")));
						}
					}
				}
				result.add(new XMLRpcCall(mName, params));

			}
			return result;
		} else {
			final List<XMLRpcCall> result = new ArrayList<XMLRpcCall>(1);
			final Element params = xml.getChild("params");
			final List<Element> listParam = params.getChildren();
			final List<Object> parameters = new ArrayList<>(listParam.size());
			for (final Element param : listParam) {
				parameters.add(XMLRpc.getValue(param.getChild("value")));
			}
			final XMLRpcCall call = new XMLRpcCall(methodName, parameters);
			result.add(call);
			return result;
		}

	}
}
