import xml.etree.ElementTree as ET

class DagTree(ET.Element):
    def insert_child(self,name,text=""):
        if text == "":
            return ET.SubElement(self,name)
        else:
            retval = ET.SubElement(self,name)
            retval.text = text
            return retval

    def get_tree(self):
        return ET.ElementTree(self)

def parse_dag(file):
    return ET.parse(file)
    
def indent_elements(elem, level=0):
    i = "\n" + level*"  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            indent_elements(elem, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def test():
    # build a tree structure
    root = DagTree("html")
    
    head = root.insert_child("head")
    
    title = root.insert_child("title")
    title.text = "Page Title"
    
    body = root.insert_child("body")
    body.set("bgcolor", "#ffffff")
    
    body.text = "Hello, World!"

    # wrap it in an ElementTree instance, and save as XML
    tree = root.get_tree()
    indent_elements(root)
    tree.write("page.xhtml")

def test2():
    from sys import argv 
    root = parse_dag(argv[1])
    root.write("page2.xhtml")
    
if __name__ == "__main__":
    test2()
