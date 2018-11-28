#Tenserflow application multilayer perceptron rock min dataset
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.utils import shuffle
import matplotlib.pyplot as plt

df = pd.read_csv("C:/Users/rohan/Desktop/uic/Deep learning/data/Rok_mine.csv")
#print(df.head())
def read_data_set(DataFrame):
    X = DataFrame[DataFrame.columns[0:60]].values
    y = DataFrame[DataFrame.columns[60]].values
    y1 = DataFrame[DataFrame.columns[60]].values
    encoder = LabelEncoder()
    encoder.fit(y)
    y =encoder.transform(y)
    Y= one_hot_encode(y)
    return X ,Y,y1

def one_hot_encode(labels):
    n_labels = len(labels)
    n_unique_labels = len(np.unique(labels))
    one_hot_encode = np.zeros((n_labels, n_unique_labels))
    one_hot_encode[np.arange(n_labels), labels] = 1
    return one_hot_encode

# shuffeling the data set.
X,y ,y1=read_data_set(df)
print(X)
X = shuffle(X,random_state=42)
y = shuffle(y,random_state =42)
# splitting data into train and split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# declareing importnat parameters....
learning_rate =0.3
training_epochs =1000
cost_history= np.empty(shape=[1],dtype=float)
ndim = X.shape[1]
n_class=2
model_path ="C:/Users/rohan/Desktop/uic/Deep learning/data/RMP"
# Defining hiddenlayers and nurons--
h_1 =60
h_2 =60
h_3 =60
h_4 =60

x = tf.placeholder(tf.float32,[None,ndim])
W = tf.Variable(tf.zeros([ndim,n_class]))
b = tf.Variable(tf.zeros([n_class]))
y_ = tf.placeholder(tf.float32,[None,n_class])



# Defining model:

def multilayerPerceptron(x,weights,biases):
    layer_1 = tf.add(tf.matmul(x,weights['h1']),biases['b1'])
    layer_1 = tf.nn.sigmoid(layer_1)

    layer_2 = tf.add(tf.matmul(layer_1,weights['h2']),biases['b2'])
    layer_2 = tf.nn.sigmoid(layer_2)

    layer_3 = tf.add(tf.matmul(layer_2, weights['h3']), biases['b3'])
    layer_3 = tf.nn.sigmoid(layer_3)

    layer_4 = tf.add(tf.matmul(layer_3, weights['h4']), biases['b4'])
    layer_4 = tf.nn.relu(layer_4)

    output = tf.add(tf.matmul(layer_4,weights['out']),biases['out'])
    return output

weights={
'h1': tf.Variable(tf.truncated_normal([ndim,h_1])),
'h2': tf.Variable(tf.truncated_normal([ndim,h_2])),
'h3': tf.Variable(tf.truncated_normal([ndim,h_3])),
'h4': tf.Variable(tf.truncated_normal([ndim,h_4])),
'out': tf.Variable(tf.truncated_normal([h_4, n_class]))
}
biases={
    'b1': tf.Variable(tf.truncated_normal([h_1])),
    'b2': tf.Variable(tf.truncated_normal([h_2])),
    'b3': tf.Variable(tf.truncated_normal([h_3])),
    'b4': tf.Variable(tf.truncated_normal([h_4])),
    'out': tf.Variable(tf.truncated_normal([n_class]))
}
# Initializing variable---- and saving the model

init = tf.global_variables_initializer()
saver = tf.train.Saver()
# finding output of perceptron
y_p= multilayerPerceptron(x,weights,biases)

#Defining costfunction and optimizer
cost_function = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=y_p,labels=y_))
training_step = tf.train.GradientDescentOptimizer(learning_rate).minimize(cost_function)
sess = tf.Session()
sess.run(init)

# Calculating cost and epochs of each iterations
mse_history =[]
accuracy_history =[]

for epoch in range(training_epochs):
    sess.run(training_step,feed_dict={x:X_train ,y_: y_train})
    cost = sess.run(cost_function,feed_dict={x: X_train,y_: y_train})
    cost_history = np.append(cost_history, cost)
    cost_prediction = tf.equal(tf.argmax(y_p,1),tf.argmax(y_,1))
    accuracy = tf.reduce_mean(tf.cast(cost_prediction,tf.float32))
    pred_y = sess.run(y_p,feed_dict={x:X_test})
    mse = tf.reduce_mean(tf.square(pred_y - y_test))
    mse_ = sess.run(mse)
    mse_history.append(mse_)
    accuracy = sess.run(accuracy,feed_dict={x:X_train,y_:y_train})
    accuracy_history.append(accuracy)
    print('epoch : ', epoch, ' - ', 'cost: ', cost, " - MSE: ", mse_, "- Train Accuracy: ", accuracy)

print(mse_history)

save_path = saver.save(sess, model_path)
#print("Model saved in file: %s" % save_path)

# Plot mse and accuracy graph



prediction = tf.argmax(y_p, 1)
correct_prediction = tf.equal(prediction, tf.argmax(y_, 1))
accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

# print (accuracy_run)
print('******************************************************')
print(" 0 Stands for M i.e. Mine & 1 Stands for R i.e. Rock")
print('******************************************************')
for i in range(93, 101):
    #print(X[i].reshape(1, 60))
    #print()
    prediction_run = sess.run(prediction, feed_dict={x: X[i].reshape(1, 60)})
    accuracy_run = sess.run(accuracy, feed_dict={x: X[i].reshape(1, 60), y_: y[i].reshape(1,2)})
    print("Original Class : ", y1[i], " Predicted Values : ", prediction_run[0], " Accuracy : ", accuracy_run)