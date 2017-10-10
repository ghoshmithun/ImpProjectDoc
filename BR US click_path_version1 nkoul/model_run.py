import time
import cPickle as pickle
import sklearn as sk
import sklearn.feature_extraction.text as txt
import sklearn.svm as svm
import sklearn.metrics as metrics

# Space Tokenizer
def cust_token(sent):
	return sent.split(' ')

# Convert the session to a string
def to_lof_strings(dat):
	los = []
	for item in dat:
		ftr_item = [it2 for it2 in item if (it2!='orderConfirm'and it2!='shipping') and it2!='billing'\
		and it2!='gifting' and it2!='orderPlacement' and it2 !='order_gateway_sign_in' and it2!=\
		'order_status_detail' and it2!='shipment_detail' and it2!='br:checkout:Checkout' and it2.lower()\
		!='completeregistration' and it2.lower() !='address_book' and it2.lower() !='br:checkout:checkout:module:shipping'\
		and it2.lower()!='single_order_detail' and it2.lower() !='br:profile:order_status_shipment_detail' and it2.lower() !='rewards']
		los.append(' '.join(ftr_item))
	return los

# Level 2 Prediction
def sequential_prediction_correction(predictor,preds,dat_x):
	new_preds = []
	for data,prediction in zip(dat_x,preds):
		if prediction == 'NP':
			new_preds.append(prediction)
		else:
			nw_pred = predictor.predict(data)[0]
			if nw_pred == 'NP':
				new_preds.append('AC')
			else:
				new_preds.append(nw_pred)
	return new_preds

# Predict with a sequence of two classifiers
def sequential_prediction(predictor1,predictor2,dat_x):
	preds = predictor1.predict(dat_x);
	new_preds = sequential_prediction_correction(predictor2,preds,dat_x)
	return new_preds

# Single Session used as test for timing the vectorizer and classifier
test_session_file = "test_session.pkl"
# Vectorizer used to generate the ngram features
vectorizer_file  = "vectorizer.pkl"
# Classifies as either potential purchaser(P) and non purchaser(NP)
classifier_l1_file = "classifier_l_one.pkl"
# Classifies the previously labeled potential purchasers as purchaser(P) or cart abandoner(NP)
classifier_l2_file = "classifier_l_two.pkl"
# Load the vectorizer and the classifier
vctr = pickle.load(open(vectorizer_file,'r'))
clf_l1 = pickle.load(open(classifier_l1_file,'r'))
clf_l2 = pickle.load(open(classifier_l2_file,'r'))
ls_test_session = to_lof_strings([pickle.load(open(test_session_file,'r'))])

# Vectorization time
vec_start_time = time.time()
vctr_input = vctr.transform(ls_test_session)
vec_end_time = time.time()
print 'Vectorization Time in seconds for one session: ' + str(vec_end_time-vec_start_time)

# Runtime for Classification
clf_start_time = time.time()
preds = sequential_prediction(clf_l1,clf_l2,vctr_input)
clf_end_time = time.time()
print 'Classifcation Time in seconds with sequential classifier : ' + str(clf_end_time-clf_start_time)



