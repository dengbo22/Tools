# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 17:01
# @Author  : changbodeng
# @FileName: feature_engine.py
import numpy as np
import scipy.sparse as sp
from sklearn.metrics import roc_auc_score, roc_curve
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier


def data2csr(data, feature_index):
    def _libsvm2csr(data_row, item_sep=' ', kv_sep=':'):
        feat = [0.0 for _ in range(len(feature_index))]
        for kv_pair in data_row.split(item_sep):
            key, *val = kv_pair.split(kv_sep)
            if len(val) == 1 and key in feature_index:
                if val[0] == '0' or val[0] == '0.0':
                    value = 0.000001
                    feat[feature_index[key]] = value
                    continue
                value = float(val[0]) if "." in val[0] or 'e' in val[0] else int(val[0])
                feat[feature_index[key]] = value

        csr_feat = sp.csr_matrix(feat)
        return csr_feat

    data['libsvm_feature'] = data['libsvm_feature'].apply(_libsvm2csr)
    return data


def cal_auc_ks(y_true, y_score):
    if len(np.unique(y_true)) == 1 or len(np.unique(y_score)) == 1:
        return 0.0, -1.0
    auc = roc_auc_score(y_true=y_true, y_score=y_score)
    fpr, tpr, thresholds = roc_curve(y_true=y_true, y_score=y_score)
    ks = max(tpr - fpr)
    return auc, ks


def build_feature_index(data):
    fea_col_set, bs = set(), 10 * 10000
    for si in range(0, len(data), bs):
        ei = min(si + bs, len(data))
        fea_col_set = fea_col_set | set.union(*data.iloc[si:ei]["libsvm_feature"].apply(
            lambda xs: set([x.split(":")[0] for x in xs.split(" ") if len(x.split(":")) == 2])).values.tolist())
    feature_index = dict(zip(sorted(list(fea_col_set)), range(len(fea_col_set))))
    return feature_index


def pseudo_train(train_data, xgb_params):
    feature_index = build_feature_index(train_data)
    # save feature_index ==> pkl.dump(feature_index, file)
    train_data = data2csr(train_data, feature_index)
    train_x, train_y = sp.vstack(
        tuple(train_data["libsvm_feature"].values.tolist()), format='csr'
    ), train_data["label"].values.tolist()
    model = XGBClassifier(**xgb_params)
    final_train_x, final_val_x, final_train_y, final_val_y = train_test_split(
        train_x, train_y, test_size=0.1, random_state=xgb_params["random_state"]
    )
    model.fit(
        final_train_x, final_train_y, eval_set=[(final_val_x, final_val_y)],
        eval_metric="auc", early_stopping_rounds=100, verbose=100
    )
    # save model ==> pkl.dump(model, file)
    return model


def pseudo_predict(model, predict_data, feature_index):
    predict_data = data2csr(predict_data, feature_index)
    predict_data = sp.vstack(tuple(predict_data["libsvm_feature"].values.tolist()), format='csr')

    score_list = model.predict_proba(predict_data)[:, 1].tolist()
    predict_data["score"] = score_list
    return predict_data


def pseudo_eval(eval_data):
    ks, auc = [], []
    for test_type in eval_data['test_type'].drop_duplicates().tolist():
        part_data = eval_data[eval_data['test_type'] == test_type]
        if len(part_data) <= 0:
            continue
        part_auc, part_ks = cal_auc_ks(part_data['label'].values, part_data['score'].values)
        if part_auc <= 0.5 and part_ks == -1.0:
            continue

        ks.append(part_ks)
        auc.append(part_auc)
    avg_ks, avg_auc = np.mean(ks), np.mean(auc)
    return float(avg_auc), float(avg_ks)
