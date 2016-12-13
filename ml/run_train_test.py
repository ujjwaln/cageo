from learn import hour
from learn.create_learning_input_file import create_learning_input
from learn.train_model import train_model
from learn.test_model import test_model
import os


curdir = os.path.dirname(__file__)
data_dir = os.path.join(curdir, "data")

#create random forest training input file
state_file_name = os.path.join(data_dir, "train.states.csv")
diff_file_name = os.path.join(data_dir, "train.diffs.csv")
train_learn_file = os.path.join(data_dir, "train.learn.%dhr.csv" % hour)
create_learning_input(state_file_name, diff_file_name, train_learn_file)

#create random forest testing input file
state_file_name = os.path.join(data_dir, "test.states.csv")
diff_file_name = os.path.join(data_dir, "test.diffs.csv")
test_learn_file = os.path.join(data_dir, "test.learn.%dhr.csv" % hour)
create_learning_input(state_file_name, diff_file_name, test_learn_file)

#train the model
train_model(train_learn_file, n_estimators=500, min_samples_leaf=1)

#test the model
testing_file_name = os.path.join(data_dir, "test.learn.%dhr.csv" % hour)
roi_output_file = os.path.join(data_dir, "roi_output.test.%dhr.csv" % hour)
test_model(testing_file_name, roi_output_file)
