import mapnik
import os
from lxml import etree
from ci.config import get_instance


def create_roi_output_style(attr_name):
    qgis_style_file = os.path.join(os.path.dirname(__file__), "plot_data/roi_pred_classes_4.qml")
    symbols = {}

    with open(qgis_style_file, 'rb') as qsf:
        doc = etree.parse(qsf)
        for range in doc.xpath('//range'):
            upper = range.attrib['upper']
            lower = range.attrib['lower']
            label = range.attrib['label']
            symbol = range.attrib['symbol']
            symbols[symbol] = {
                "lower": float(lower),
                "upper": float(upper),
                "label": str(label),
            }

        for node in doc.xpath("//prop[@k='color']"):
            layer = node.getparent()
            sym = layer.getparent()
            sym_name = sym.attrib['name']
            str_color = node.attrib["v"]
            c = str_color.split(",")
            symbols[sym_name]["color"] = mapnik.Color("rgb(%d,%d,%d)" % (int(c[0]), int(c[1]), int(c[2])))

    style = mapnik.Style()

    for sname in symbols:
        r = mapnik.Rule()
        psym = mapnik.MarkersSymbolizer()
        psym.fill = symbols[sname]["color"]
        psym.width = mapnik.Expression('6')
        psym.height = mapnik.Expression('6')
        psym.stroke = mapnik.Stroke(mapnik.Color('white'), 0)
        r.symbols.append(psym)

        upper = symbols[sname]["upper"]
        lower = symbols[sname]["lower"]
        f = mapnik.Filter("[%s] >= %f and [%s] < %f" % (attr_name, lower, attr_name, upper))
        r.filter = f

        style.opacity = 0.8
        style.rules.append(r)
    return style


def create_map(dir, roi_file):

    width = 800
    height = 800
    config = get_instance()
    lats = config.bbox["lats"]
    lons = config.bbox["lons"]
    buff = 1
    bbox = mapnik.Box2d(lons[0]-buff, lats[0]-buff, lons[1]+buff, lats[1]+buff)

    map = mapnik.Map(width, height)

    plot_data_dir = os.path.join(os.path.dirname(__file__), "plot_data")
    mapnik.load_map(map, os.path.join(plot_data_dir, "ci_test.xml"))

    roi_csv_file = os.path.join(os.path.dirname(__file__), dir, roi_file)
    roi_csv_ds = mapnik.CSV(file=roi_csv_file)
    roi_layer = mapnik.Layer("roi_pred")
    roi_layer.datasource = roi_csv_ds

    roi_layer_style = create_roi_output_style("prediction")
    map.append_style("roi_layer_style", roi_layer_style)

    roi_layer.styles.append("roi_layer_style")

    map.layers.append(roi_layer)

    map.zoom_to_box(bbox)
    mapnik.render_to_file(map, os.path.join(dir, '%s.png' % roi_file), 'png')


if __name__ == "__main__":
    dir = 'data_07_24_1530'
    for hr in range(0, 5):
        roi_file = "roi_output.test.%dhr.csv" % hr
        create_map(dir, roi_file)
