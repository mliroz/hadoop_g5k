import os
import re
import shutil
import tempfile

import xml.etree.ElementTree as ET


# XML conf files ##############################################################

def create_xml_file(f):
    with open(f, "w") as fout:
        fout.write("<configuration>\n")
        fout.write("</configuration>")


def read_param_in_xml_file(f, name, default=None):
    tree = ET.parse(f)
    root = tree.getroot()
    res = root.findall("./property/[name='%s']/value" % name)
    if res:
        return res[0].text
    else:
        return default


def read_in_xml_file(f, param_names):

    if not param_names:
        return {}

    local_param_names = list(param_names)

    tree = ET.parse(f)
    root = tree.getroot()

    params = {}
    for name in local_param_names:
        res = root.findall("./property/[name='%s']/value" % name)
        # Warning multiple parameters?
        if res:
            params[name] = res[-1].text

    return params


def __replace_line(line, value):
    return re.sub(r'(.*)<value>[^<]*</value>(.*)', r'\g<1><value>' + value +
                  r'</value>\g<2>', line)


def replace_in_xml_file(f, name, value,
                        create_if_absent=False, replace_if_present=True):
    """Assign the given value to variable name in xml file f.

    Args:
      f (str):
        The path of the file.
      name (str):
        The name of the variable.
      value (str):
        The new value to be assigned:
      create_if_absent (bool, optional):
        If True, the variable will be created at the end of the file in case
        it was not already present.
      replace_if_present (bool, optional):
        If True, the current value will be replaced; otherwise, the current
        value will be maintained and the specified value ignored .

    Returns (bool):
      True if the assignment has been made, False otherwise.
    """

    current_value = read_param_in_xml_file(f, name)

    if current_value:
        if replace_if_present:
            (_, temp_file) = tempfile.mkstemp("", "xmlf-", "/tmp")
            with open(f) as in_file, open(temp_file, "w") as out_file:

                # Search property (we know it exists)
                line = in_file.readline()
                while "<name>" + name + "</name>" not in line:
                    out_file.write(line)
                    line = in_file.readline()

                # Replace with new value
                if "<value>" in line:
                    out_file.write(__replace_line(line, value))
                else:
                    out_file.write(line)
                    line = in_file.readline()
                    out_file.write(__replace_line(line, value))

                # changed = True

                # Write the rest of the file
                line = in_file.readline()
                while line != "":
                    out_file.write(line)
                    line = in_file.readline()

        else:
            return False
    else:
        if create_if_absent:
            (_, temp_file) = tempfile.mkstemp("", "xmlf-", "/tmp")
            with open(f) as in_file, open(temp_file, "w") as out_file:

                # Search end of file
                line = in_file.readline()
                while "</configuration>" not in line:
                    out_file.write(line)
                    line = in_file.readline()
                out_file.write("  <property><name>" + name + "</name>" +
                           "<value>" + str(value) + "</value></property>\n")
                out_file.write(line)
                # changed = True

        else:
            return False

    shutil.copyfile(temp_file, f)
    os.remove(temp_file)
    return True


# Properties files ############################################################

def __parse_props_line(line):

    if line.startswith("#"):
        return None, None
    else:
        parts = line.split()
        if parts:
            return parts
        else:
            return None, None


def read_param_in_props_file(f, name, default=None):

    with open(f) as in_file:
        for line in in_file:
            (pname, pvalue) = __parse_props_line(line)
            if name and pname == name:
                return pvalue

    return default


def read_in_props_file(f, param_names=None):

    params = {}

    with open(f) as in_file:
        for line in in_file:
            (pname, pvalue) = __parse_props_line(line)
            if pname:
                if param_names is None or pname in param_names:
                    params[pname] = pvalue

    return params


def write_in_props_file(f, name, value, create_if_absent=False, override=True):

    current_value = read_param_in_props_file(f, name)

    if current_value:
        if override:
            (_, temp_file) = tempfile.mkstemp("", "xmlf-", "/tmp")
            with open(f) as in_file, open(temp_file, "w") as out_file:

                # Search property (we know it exists)
                line = in_file.readline()
                (pname, pvalue) = __parse_props_line(line)
                while not pname or pname != name:
                    out_file.write(line)
                    line = in_file.readline()
                    (pname, pvalue) = __parse_props_line(line)

                # Replace with new value
                out_file.write(pname + "\t" + str(value) + "\n")

                # Write the rest of the file
                line = in_file.readline()
                while line != "":
                    out_file.write(line)
                    line = in_file.readline()

            shutil.copyfile(temp_file, f)
            os.remove(temp_file)
            return True

        else:
            return False
    else:
        if create_if_absent:
            with open(f, "a") as out_file:
                out_file.write(name + "\t" + str(value) + "\n")
            return True

        else:
            return False
