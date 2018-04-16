# Efficient Graph Kernels for RDF data using Spark

## Group and Member
* Group number: 3
* Bernhard Japes  
* Shinho Kang

## Files

### Kernels

* RDFFastGraphKernel: First version of the kernel implementation. Multi-depth applicable, but still use scala iterable which is not distributed operations
`sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/kernel/RDFFastGraphKernel.scala`

* RDFFastTreeGraphKernel: Fully distributed operations only with depth 1 tree kernel
`sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/kernel/RDFFastTreeGraphKernel.scala`

* RDFFastTreeGraphKernel_v2: Fully distributed operations with multi-depth applicable
`sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/kernel/RDFFastTreeGraphKernel_v2.scala`

### Utility

* Utility object which contains functions for easy using implemented kernels.
`sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/kernel/RDFFastGraphKernelUtil.scala`

### Apps
* Usages which do experiments using implemented kernels  
`sansa-ml-spark/src/main/scala/net/sansa_stack/ml/spark/kernel/*App.scala`

### Report
* `documentation/fast_graph_kernel.pdf`: final report
* `documentation/fast_graph_kernel.zip`: archive of LaTex files

