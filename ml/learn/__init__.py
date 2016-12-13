import yaml
import os

CI_TYPE = 1
NON_CI_TYPE = 0
DAT_BEGIN_VAR = "ASPECT_count"

roi_items = None
predictor_map = None
model_file_name = None
feature_importance_filename = None


def setup_globals(hour=0):

    global roi_items
    global predictor_map
    global model_file_name
    global feature_importance_filename

    pred_config = os.path.join(os.path.dirname(__file__), "predictor_config/%d_hr.yml" % hour)
    obj = yaml.load(file(pred_config, 'rb'))

    roi_items = obj['roi_items']
    predictor_map = obj['predictor_map']

    data_dir = os.path.join(os.path.dirname(__file__), "../data")
    model_file_name = os.path.join(data_dir, "rf_model_%dhr" % hour)
    feature_importance_filename = os.path.join(data_dir, "rf_feature_imp_%d.txt" % hour)

#set up
hour = 0
setup_globals(hour=hour)
