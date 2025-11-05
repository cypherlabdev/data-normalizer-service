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

## Tickets to Create

### 1. [NEW] Implement Service Layer and Normalization Logic (P0)
- Add service layer with odds normalization algorithms
- Implement data validation
- Add deduplication logic

### 2. [NEW] Create README and Documentation (P1)
- Document normalization algorithms
- Configuration guide

### 3. [NEW] Add Comprehensive Metrics (P2)
- Normalization metrics, error rates, latency
