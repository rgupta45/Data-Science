import tensorflow as tf
#Basic------------
#a = tf.constant(5)
#b = tf.constant(2)
#c = tf.constant(3)
#d = tf.multiply(a,b)
#e =tf.add(c,b)
#f = tf.subtract(d,e)
#sess = tf.Session()
#print(sess.run(f))
#print(node_1,node_2)

#Placeholders------------

#a= tf.placeholder(tf.float32)
#b= tf.placeholder(tf.float32)
#node_add = a+b

#sess= tf.Session()
#print(sess.run(node_add,{a:[1,3],b:[2,4]}))

# Variables Creating a linear model--------------------------
W = tf.Variable([.3],tf.float32)
b = tf.Variable([-.3],tf.float32)
x = tf.placeholder(tf.float32)
linear_model =  W*x + b
init =tf.global_variables_initializer()
sess = tf.Session()
#sess.run(init)
#print(sess.run(linear_model,{x:[1,2,3,4]}))

#evaluating model ------------------------------
y= tf.placeholder(tf.float32)
square_delta= tf.square(linear_model-y)
loss = tf.reduce_sum(square_delta)
#print(sess.run(loss,{x:[1,2,3,4],y:[0,-1,-2,-3]}))

# Step 3 how to reduce the loss-------------

optimizer = tf.train.GradientDescentOptimizer(0.01)
train = optimizer.minimize(loss)
sess.run(init)
for i in range(1000):
    sess.run(train,{x:[1,2,3,4],y:[0,-1,-2,-3]})
print(sess.run([W,b]))









