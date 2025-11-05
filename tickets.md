# data-normalizer-service - Ticket Tracking

## Service Overview
**Repository**: github.com/cypherlabdev/data-normalizer-service
**Purpose**: Consume raw odds from odds-adapter, normalize/aggregate, publish to normalized_odds topic
**Implementation Status**: 60% complete (Kafka integration done, missing service layer and validation)
**Language**: Go 1.21+

## Current Implementation

### ✅ Completed (60%)
- Kafka consumer and producer (fully implemented with testcontainers)
- Odds aggregation logic
- Models and interfaces

### ❌ Missing (40%)
- No service layer (direct aggregator usage)
- No data normalization logic
- No validation
- No README
- No metrics

## Existing Asana Tickets

### 1. [1211394356065966] ENG-70: Data Normalizer
**Task ID**: 1211394356065966
**ENG Field**: ENG-70
**URL**: https://app.asana.com/0/1211254851871080/1211394356065966
**Type**: feature
**Assignee**: sj@cypherlab.tech
**Dependencies**: ⬆️ ENG-66 (odds-adapter), ⬇️ ENG-74 (odds-optimizer)

## Proposed New Tickets (Created in Asana)

### 2. Implement Service Layer and Normalization Logic
**Task ID**: 1211847710933308
**ENG**: Pending assignment
**URL**: https://app.asana.com/0/1211254851871080/1211847710933308
**Priority**: P0
**Type**: feature

### 3. Create README and Documentation
**Task ID**: 1211847648310944
**ENG**: Pending assignment
**URL**: https://app.asana.com/0/1211254851871080/1211847648310944
**Priority**: P1
**Type**: documentation

### 4. Add Comprehensive Metrics
**Task ID**: 1211847631576879
**ENG**: Pending assignment
**URL**: https://app.asana.com/0/1211254851871080/1211847631576879
**Priority**: P2
**Type**: feature
