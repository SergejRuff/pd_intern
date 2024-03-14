import matplotlib.pyplot as plt
import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV, RandomizedSearchCV
from sklearn.tree import DecisionTreeClassifier, plot_tree
from sklearn.metrics import roc_auc_score, roc_curve, accuracy_score, precision_score, recall_score, f1_score, classification_report
import os
import imblearn
import sys

print("PROCESS: Import Data")
sys.stdout.flush()
# Define random state for reproducibility
random_state = 6
output_folder = "/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/decisiontree/encoded_test"

df = pd.read_parquet("/hpc/scratch/project/ag-ixplain-cds/sergejruff/output/labed_data/string8_split.parquet")

df = df.T

print("PROCESS: split label and feature")
sys.stdout.flush()

print("DataFrame Shape:", df.shape)

# Map 'early' to 0 and 'late' to 1 in the target variable
#df['ageonset'] = df["ageonset"].map({'early-onset': 0, 'late-onset': 1})

# Separate features and target variable
X = df.drop("ageonset", axis=1)
y = df["ageonset"]

# Perform one-hot encoding
#X = pd.get_dummies(X,dtype=int)

print("PROCESS: Split data into test, val and train")
sys.stdout.flush()
# Split data into training and testing sets
X_train, X_temp, y_train, y_temp = train_test_split(X, y, train_size=0.7, random_state=random_state, stratify=y)

# Further split the temporary data into validation and test sets
X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, train_size=0.5, random_state=random_state, stratify=y_temp)

print("Number of samples in training set:", len(X_train))
print("Number of samples in validation set:", len(X_val))
print("Number of samples in test set:", len(X_test))

del df

#print("Process oversampling")
#sys.stdout.flush()
# Apply oversampling to the minority class
#oversample_method = imblearn.over_sampling.RandomOverSampler(sampling_strategy='minority', random_state=random_state)
#X_train, y_train = oversample_method.fit_resample(X_train, y_train)

print("PROCESS: Hyperparametertuning the tree")
sys.stdout.flush()
# Define the decision tree classifier
clf = DecisionTreeClassifier(criterion="entropy",random_state=random_state)

# Define hyperparameters grid
param_grid = {
    'min_samples_leaf': [6, 8],
    'max_depth': [4, 5, 8]
   
}

# Grid Search Cross Validation
grid_search = GridSearchCV(clf, param_grid=param_grid, cv=5)
grid_search.fit(X_train, y_train)

# Extract the best model
best_model = grid_search.best_estimator_

# Print the best parameters found
print("Best Parameters:", grid_search.best_params_)

# Print the best score found
print("Best Score:", grid_search.best_score_)

print("PROCESS: Train data with best hyperparameters")
sys.stdout.flush()
# Train decision tree using the best parameters from both searches
dt_best = DecisionTreeClassifier(**grid_search.best_params_,criterion="entropy", random_state=random_state)
dt_best.fit(X_train, y_train)

print("PROCESS: evaluate on val")
sys.stdout.flush()

# Evaluate the best model
dt_score = dt_best.score(X_val, y_val)
print("Validation Accuracy:", dt_score)

# Plot ROC curve
y_pred_proba = dt_best.predict_proba(X_val)[:, 1]
auc = roc_auc_score(y_val, y_pred_proba)
print('ROC AUC=%.3f' % (auc))

# False positive rate und true positive rate berechnen
fpr, tpr, thresholds = roc_curve(y_val, y_pred_proba)
plt.plot(fpr, tpr, label='Decision Tree')
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('ROC Curve')
plt.grid(True)
plt.legend()

# Save ROC curve plot as PNG

if not os.path.exists(output_folder):
    os.makedirs(output_folder)
plt.savefig(os.path.join(output_folder, "roc_curve.png"))
plt.close()

# Get feature importances
feature_importances = dt_best.feature_importances_

# Get the indices of the top 20 features
top_20_indices = feature_importances.argsort()[-20:][::-1]

# Get the feature names
top_20_features = X.columns[top_20_indices]

# Get the importances of top 20 features
top_20_importances = feature_importances[top_20_indices]

# Plot the top 20 features and their importances
plt.figure(figsize=(10, 6))
plt.barh(range(len(top_20_importances)), top_20_importances, align='center')
plt.yticks(range(len(top_20_importances)), top_20_features)
plt.xlabel('Feature Importance')
plt.ylabel('Feature')
plt.title('Top 20 Features Importances')

# Save top 20 features importances plot as PNG
plt.savefig(os.path.join(output_folder, "top_20_features_importances.png"))
plt.close()

print("PROCESS: Test on testdata")
sys.stdout.flush()
# Predict on the test set
y_pred = dt_best.predict(X_test)

# Calculate evaluation metrics
accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)

# Print evaluation metrics
print("Accuracy:", accuracy)
print("Precision:", precision)
print("Recall (Sensitivity):", recall)
print("F1 Score:", f1)

# Print classification report
classification_rep = classification_report(y_test, y_pred)
print("Classification Report:")
print(classification_rep)



# Save evaluation metrics to a text file
with open(os.path.join(output_folder, "evaluation_metrics.txt"), "w") as text_file:
    text_file.write("Best Parameters: {}\n".format(grid_search.best_params_))
    text_file.write("Best Score: {}\n".format(grid_search.best_score_))
    text_file.write(f"Number of samples in training set: {len(X_train)}\n")
    text_file.write(f"Number of samples in validation set: {len(X_val)}\n")
    text_file.write(f"Number of samples in test set: {len(X_test)}\n")
    text_file.write(f"Accuracy: {accuracy}\n")
    text_file.write(f"Precision: {precision}\n")
    text_file.write(f"Recall (Sensitivity): {recall}\n")
    text_file.write(f"F1 Score: {f1}\n")
    text_file.write("Classification Report:\n")
    text_file.write(classification_rep)


# Visualize the decision tree
plt.figure(figsize=(12, 8))
plot_tree(dt_best, filled=True, feature_names=X.columns, class_names=['early', 'late'])
plt.title('Decision Tree Visualization')

# Save decision tree visualization as PNG
plt.savefig(os.path.join(output_folder, "decision_tree_visualization.png"))
plt.close()
print("END OF THE SCRIPT")

