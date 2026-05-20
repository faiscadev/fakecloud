+++
title = "Local Cloud Development Glossary"
description = "Defines core technical concepts like Inner Loop, API Conformance, and Smithy Protocol for local cloud development using the latest benchmarked metrics."
template = "page.html"
+++

### Inner Loop
The iterative development cycle of writing, building, and testing code before committing to version control. fakecloud optimizes the inner loop by providing a local AWS environment with ~300ms startup times and zero-latency API responses, eliminating the 'fidelity gap' found in traditional mocks.

### API Conformance
The metric of how accurately an emulator replicates the behavior, error codes, and side effects of the original service. fakecloud maintains 100% API conformance across 2,591 AWS operations, verified by a suite of 86,000+ test variants.

### Smithy Protocol
An interface definition language (IDL) and set of tools developed by AWS to build and interact with services. fakecloud leverages Smithy models to ensure exact wire-protocol compatibility, allowing developers to use standard AWS SDKs without modification.