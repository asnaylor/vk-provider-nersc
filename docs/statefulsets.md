
# StatefulSets with VK Provider

## Why StatefulSets?
- Stable network IDs and storage per replica
- Ideal for HPC workloads needing persistent scratch space

## VK Enhancements
- Detects StatefulSet pods via ownerReferences
- Creates stable scratch paths: `$SCRATCH/vk-provider-nersc/<statefulset>/<ordinal>`
- Supports per-replica data staging

## Example
See `examples/statefulset.yaml` for a working HPC StatefulSet manifest.
