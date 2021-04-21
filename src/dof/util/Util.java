package dof.util;

public class Util {

	public static boolean contains(String[] ar, String el) {
		for (String s : ar) {
			if (s.equals(el))
				return true;
		}
		return false;
	}

	public static String findByStart(String[] ar, String start) {
		for (String s : ar) {
			if (s.startsWith(start))
				return s;
		}
		return null;
	}

	protected boolean containsAny(String[] ar, String... els) {
		for (String e : els) {
			if (contains(ar, e))
				return true;
		}
		return false;
	}

	public static String parseDefaultStringArg(String[] args, String arg, String defaultValue) {
		String s = Util.findByStart(args, arg);
		if (s == null)
			return defaultValue;
		String[] ss = s.split("=");
		if (ss.length == 1)
			return null;
		return ss[1];
	}


	public static boolean parseDefaultBooleanArg(String[] args, String arg, boolean defaultValue) {
		String s = Util.findByStart(args, arg);
		if (s == null)
			return defaultValue;
		String[] ss = s.split("=");
		if (ss.length == 1)
			return true;
		boolean b = Boolean.parseBoolean(ss[1]);
		return b;
	}

}
