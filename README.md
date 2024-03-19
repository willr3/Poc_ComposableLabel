# horreum composable labels

This project is a PoC for Horreum composable labels

It is meant to test the viability of composable labels as a replacement for the current data model.
The normal entiteis are simplified.
* Schema is removed
* Test contains the Labels

Target Features:

3 Extractors
- [x] normal jsonpath extractor (JsonpathExtractor)
- [x] label based extractor (LabelValueExtractor)
- [ ] run metadata (column field) extractor (RunMetatdataExtractor)

Validation:
- [x] Processing labels by order of dependency (needs tests)
- [x] loop detection (Label.isCircular)

Label_Value creation:
* with & without transform (javascript)   
* single & multi extractor (single uses the identity and multi creates an object)
* iterating along size scalar extractor values (First or All)
* multi-iterating (Length or NxN)

 any of the combinations that involve iterating require tracking the source label_value indexes for subsequent label_values



