import numpy as np
import networkx as nx
import matplotlib.pyplot as plt

#
def load_dat(filename):
	count_dat = []
	fh = open(filename,'r')
	names = fh.readline().rstrip().split(',')
	for line in fh:
		count_dat.append(line.rstrip().split(','))
	fh.close()
	return count_dat,names

#
def convert_np(count_dat):
	size_mtx = len(count_dat[0])
	counts_np = np.zeros((size_mtx,size_mtx))
	for index1,item1 in enumerate(count_dat):
		for index2,item2 in enumerate(item1):
			counts_np[index1][index2] = item2
	return counts_np

#
def convert_to_graph(count_dat,names):
	ndict = {}
	for index,item in enumerate(names):
		ndict[index] = item.lower()
	all_triples = []
	Gx = nx.Graph()
	Gx.add_nodes_from(map(str.lower,names))
	for itr1 in range(0,len(names)):
		for itr2 in range(0,len(names)):
			if float(count_dat[itr1][itr2]) != 0.0:
				all_triples.append((ndict[itr1],ndict[itr2],float(count_dat[itr1][itr2])))
	Gx.add_weighted_edges_from(all_triples)
	return Gx

#
def build_name_dict(names):
	ndict = {}
	for name in names:
		ndict[name] = name
	return ndict

#
def build_edge_list(names):
	edge_list = []
	for item1 in names:
		for item2 in names:
			edge_list.append((item1,item2))
	return edge_list

#
def sort_dict_by_val(rd):
	s_dict = sorted(rd.iteritems(),key=lambda(k,v):(v,k),reverse=True)
	return s_dict

#
def gen_node_sizes(scores,base_size=1000):
	max_score = scores[0]
	node_sizes = [500+(item/max_score)*base_size for item in scores]
	return node_sizes

# Draw sorted subgraph with node sizes as determined by the scores.
def draw_sorted_subgraph(grph,t_ct=10):
	node_scores = nx.pagerank(grph)
	s_node_scores = sort_dict_by_val(node_scores)
	ccount = 0
	top_nodes = []; top_node_scores = []
	for item in s_node_scores:
		if ccount <= t_ct:
			top_nodes.append(item[0])
			top_node_scores.append(item[1])
			ccount += 1
	vtx_names_dict = build_name_dict(top_nodes)
	edge_list = build_edge_list(top_nodes)
	node_sizes = gen_node_sizes(top_node_scores,4000)
	nx.draw(grph,labels=vtx_names_dict,nodelist=top_nodes,edgelist=edge_list,node_size=node_sizes)
	plt.show()
	return None



