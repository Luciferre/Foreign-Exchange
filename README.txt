decisiontree
    DTBuilder.java

preparedata
    FeatureBuilder.java

randomforest
    RFBuilder.java
    Test.java

The DTBuilder.java is used for build decision tree from training data.

The FeatureBuilder.java aims to preprocess the raw data and prepare the data to be used for decision tree.

The RFBuilder.java builds a random forest based on decision tree, it finally serialize the forest to text file.

The Test.java deserializes the forest and tests the remaining 20% test records.