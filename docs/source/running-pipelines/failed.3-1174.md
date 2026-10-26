failed.3-1174.md 
## Error signatures

1174 tasks, 2 signatures

| tasks | signature |
|---:|---|
| 705 | `java.lang.OutOfMemoryError: Java heap space` |
| 469 | `java.lang.OutOfMemoryError: Required array length <n> + <n> is too large` |

## Tails

### 153. msdmo-HS-PXD004352-20150112_EXQ00_JaRi_SA_Sups_mDC_n_01.TnP-s2

tail -50 .drypipe/msdmo-HS-PXD004352-20150112_EXQ00_JaRi_SA_Sups_mDC_n_01.TnP-s2/out.log

```10% 20% 30% 40% 50% 60% 70% 80% 90% 100%
Mon Sep 28 02:20:07 EDT 2026 Exporting. Please Wait...
10% 20% 30% 40% 50% 60% 70% 80% 90%<CompomicsError>PeptideShaker used up all the memory and had to be stopped. See the PeptideShaker log for details.</CompomicsError>
Ran out of memory!
Memory given to the Java virtual machine: 15032385536.
Memory used by the Java virtual machine: 15032385536.
Free memory in the Java virtual machine: 3978291240.
java.lang.OutOfMemoryError: Java heap space
	at java.base/java.util.Arrays.copyOf(Arrays.java:3537)
	at java.base/java.io.ByteArrayOutputStream.ensureCapacity(ByteArrayOutputStream.java:100)
	at java.base/java.io.ByteArrayOutputStream.write(ByteArrayOutputStream.java:130)
``` 

### 542. msdmo-HS-PXD004352-20150112_EXQ00_JaRi_SA_Sups_mDC_n_02.TnP-s3

``` 
10% 20% 30% 40% 50% 60% 70% 80%<CompomicsError>PeptideShaker used up all the memory and had to be stopped. See the PeptideShaker log for details.</CompomicsError>
Ran out of memory!
Memory given to the Java virtual machine: 15032385536.
Memory used by the Java virtual machine: 15032385536.
Free memory in the Java virtual machine: 1638666024.
java.lang.OutOfMemoryError: Required array length 2147483639 + 31 is too large
    at java.base/jdk.internal.util.ArraysSupport.hugeLength(ArraysSupport.java:649)
    at java.base/jdk.internal.util.ArraysSupport.newLength(ArraysSupport.java:642)
    at java.base/java.io.ByteArrayOutputStream.ensureCapacity(ByteArrayOutputStream.java:100)
    at java.base/java.io.ByteArrayOutputStream.write(ByteArrayOutputStream.java:130)
    at java.base/java.io.ObjectOutputStream$BlockDataOutputStream.drain(ObjectOutputStream.java:1886)
    at java.base/java.io.ObjectOutputStream$BlockDataOutputStream.setBlockDataMode(ObjectOutputStream.java:1795)
    at java.base/java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1192)
``` 