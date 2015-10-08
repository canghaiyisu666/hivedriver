package util;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.commons.lang.StringUtils;

public class ConfigContext {
	private static ConfigContext instance = null;

	private String propertyFileName = "config";
	private ResourceBundle resourceBundle = ResourceBundle
			.getBundle(propertyFileName);

	private ConfigContext() {
		super();
	}

	public static ConfigContext getInstance() {
		if (instance == null) {
			return new ConfigContext();
		}
		return instance;
	}

	public String getString(String key) {
		if (key == null || key.equals("") || key.equals("null")) {
			return "";
		}
		String result = "";
		try {
			result = resourceBundle.getString(key);
		} catch (MissingResourceException e) {
			e.printStackTrace();
		}
		return result;
	}

	public Integer getInteger(String key) {
		String value = getString(key);
		if (StringUtils.isNotBlank(value)) {
			try {
				return Integer.parseInt(value);
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
				return null;
			}
		}
		return null;
	}

	public Double getDouble(String key) {
		String value = getString(key);
		if (StringUtils.isNotBlank(value)) {
			try {
				return Double.parseDouble(value);
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
				return null;
			}
		}
		return null;
	}

	public Boolean getBoolean(String key) {
		String value = getString(key);
		if (StringUtils.isNotBlank(value)) {
			try {
				return Boolean.parseBoolean(value);
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
				return null;
			}
		}
		return null;
	}
}