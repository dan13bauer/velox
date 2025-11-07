# Dual-Server Performance Analysis Report

## Executive Summary

Analysis of a dual-server configuration where a single client simultaneously communicates with two independent servers. Each server measures the **WaitingForSendComplete → ReadyToTransfer** transition, while the client measures **WaitingForData → ReadyToReceive** transitions.

**Key Finding**: In a dual-server scenario, the client achieves 6-31% higher average throughput than individual servers, with peak performance reaching 221 GB/s. However, server asymmetry is significant, with performance differences up to 2.14x between servers at certain buffer sizes.

---

## 1. Data Collection Overview

### Configuration
- **Setup**: Single client connecting to 2 independent servers simultaneously
- **Measurement Points**:
  - **Server Side**: WaitingForSendComplete → ReadyToTransfer (sender readiness)
  - **Client Side**: WaitingForData → ReadyToReceive (receiver readiness)
- **Buffer Sizes Tested**: 512B, 1024B, 2048B, 4096B, 8192B

### Data Volume
| Entity | Transitions | Total Data | Avg per Transition |
|--------|------------|----------|-------------------|
| Server 1 | 75 | 104.43 GB | 1.39 GB |
| Server 2 | 75 | 104.43 GB | 1.39 GB |
| Client | 150 | 208.86 GB | 1.39 GB |
| **Total** | **300** | **417.72 GB** | **1.39 GB** |

**Note**: Client processes data from both servers, resulting in 2x the transition count.

---

## 2. Overall Performance Metrics

### Throughput Summary

| Metric | Server 1 | Server 2 | Client |
|--------|----------|----------|--------|
| **Min** | 910.8 MB/s | 868.9 MB/s | 869.1 MB/s |
| **Avg** | 36,802.0 MB/s | 39,527.4 MB/s | 46,363.8 MB/s |
| **Max** | 216,616.0 MB/s | 219,339.0 MB/s | 221,203.0 MB/s |
| **Variance** | 237.8x | 252.4x | 254.5x |

**Key Observations**:
- Client average throughput: **6-10% higher** than servers
- Peak throughput consistently ~220 GB/s across all entities
- High variance (237-254x) indicates significant performance fluctuation
- Minimum throughput consistently ~870 MB/s (initialization overhead)

---

## 3. Performance by Buffer Size

### Server 1 Performance

| Buffer | Records | Avg TP | Min TP | Max TP | Variance |
|--------|---------|--------|--------|--------|----------|
| 512B | 38 | 26.6 GB/s | 0.9 GB/s | 167.0 GB/s | 183.4x |
| 1024B | 19 | 36.9 GB/s | 1.5 GB/s | 109.5 GB/s | 71.7x |
| 2048B | 10 | 34.5 GB/s | 2.6 GB/s | 109.8 GB/s | 42.9x |
| **4096B** | 5 | **102.3 GB/s** ⭐ | 4.1 GB/s | 216.6 GB/s | 53.4x |
| 8192B | 3 | 64.2 GB/s | 11.4 GB/s | 153.7 GB/s | 13.4x |

### Server 2 Performance

| Buffer | Records | Avg TP | Min TP | Max TP | Variance |
|--------|---------|--------|--------|--------|----------|
| 512B | 38 | 26.1 GB/s | 0.9 GB/s | 170.2 GB/s | 196.1x |
| 1024B | 19 | 47.4 GB/s | 1.6 GB/s | 205.5 GB/s | 128.8x |
| 2048B | 10 | 52.2 GB/s | 2.6 GB/s | 197.7 GB/s | 74.9x |
| 4096B | 5 | 94.9 GB/s | 4.2 GB/s | 219.3 GB/s | 52.6x |
| 8192B | 3 | **25.5 GB/s** ⚠️ | 8.7 GB/s | 51.3 GB/s | 5.9x |

### Client Performance

| Buffer | Records | Avg TP | Min TP | Max TP | Variance |
|--------|---------|--------|--------|--------|----------|
| 512B | 76 | 30.0 GB/s | 0.9 GB/s | 181.0 GB/s | 208.3x |
| 1024B | 38 | 50.1 GB/s | 1.5 GB/s | 217.8 GB/s | 142.6x |
| 2048B | 20 | 60.2 GB/s | 2.6 GB/s | 202.0 GB/s | 77.9x |
| **4096B** | 10 | **124.1 GB/s** ⭐ | 4.1 GB/s | 221.2 GB/s | 54.4x |
| 8192B | 6 | 54.4 GB/s | 8.7 GB/s | 154.1 GB/s | 17.8x |

**Buffer Size Rankings** (by average throughput):
1. 🥇 Client 4096B: **124.1 GB/s**
2. 🥈 Server 1 4096B: 102.3 GB/s
3. 🥉 Server 2 4096B: 94.9 GB/s

---

## 4. Server Asymmetry Analysis

### Server-to-Server Comparison (Same Buffer Size)

| Buffer | Server 1 | Server 2 | Ratio | Difference |
|--------|----------|----------|-------|-----------|
| 512B | 26.6 GB/s | 26.1 GB/s | 0.98x | -0.5 GB/s |
| 1024B | 36.9 GB/s | 47.4 GB/s | 1.28x | +10.5 GB/s (+28%) |
| 2048B | 34.5 GB/s | 52.2 GB/s | 1.52x | +17.7 GB/s (+51%) |
| 4096B | 102.3 GB/s | 94.9 GB/s | 0.93x | -7.4 GB/s |
| 8192B | 64.2 GB/s | 25.5 GB/s | 0.40x | -38.7 GB/s (-60%) ⚠️ |

**Key Findings**:
- **Largest asymmetry**: 8192B buffer (server_1 is **2.52x faster** than server_2)
- **Moderate asymmetry**: 2048B buffer (server_2 is 1.52x faster)
- **Most balanced**: 512B buffer (0.98x ratio - essentially equal)
- Server 2 generally performs better at smaller buffers (512B-2048B)
- Server 1 performs better at larger buffers (4096B-8192B)

---

## 5. Client vs Servers Comparison

### Client Performance Relative to Servers

#### By Buffer Size

**512B Buffers**:
- Client: 30.0 GB/s
- Server 1: 26.6 GB/s (Client **1.13x faster**)
- Server 2: 26.1 GB/s (Client **1.15x faster**)
- Client advantage: +13-15%

**1024B Buffers**:
- Client: 50.1 GB/s
- Server 1: 36.9 GB/s (Client **1.36x faster**)
- Server 2: 47.4 GB/s (Client **1.06x faster**)
- Client advantage: +6-36% ⚠️ High variance

**2048B Buffers**:
- Client: 60.2 GB/s
- Server 1: 34.5 GB/s (Client **1.75x faster**)
- Server 2: 52.2 GB/s (Client **1.15x faster**)
- Client advantage: +15-75% ⚠️ High variance

**4096B Buffers**:
- Client: 124.1 GB/s ⭐ **PEAK**
- Server 1: 102.3 GB/s (Client **1.21x faster**)
- Server 2: 94.9 GB/s (Client **1.31x faster**)
- Client advantage: +21-31%

**8192B Buffers**:
- Client: 54.4 GB/s
- Server 1: 64.2 GB/s (Client **0.85x** - Server 1 **1.18x faster**)
- Server 2: 25.5 GB/s (Client **2.14x faster**)
- Status: Mixed - client outperforms server_2, but not server_1

### Overall Client Performance Advantage

| Aspect | Advantage |
|--------|-----------|
| Average throughput | **6-10% higher** |
| Peak throughput | **1-4 GB/s higher** |
| Consistency | More stable variance profile |
| Optimal buffer | 4096B (same as servers) |

---

## 6. Performance Distribution Analysis

### Server 1 Distribution

| Throughput Tier | Count | Percentage |
|-----------------|-------|-----------|
| < 10 GB/s | 24 | 32.0% |
| 10-50 GB/s | 20 | 26.7% |
| 50-150 GB/s | 19 | 25.3% |
| > 150 GB/s | 12 | 16.0% |

### Server 2 Distribution

| Throughput Tier | Count | Percentage |
|-----------------|-------|-----------|
| < 10 GB/s | 24 | 32.0% |
| 10-50 GB/s | 19 | 25.3% |
| 50-150 GB/s | 18 | 24.0% |
| > 150 GB/s | 14 | 18.7% |

### Client Distribution

| Throughput Tier | Count | Percentage |
|-----------------|-------|-----------|
| < 10 GB/s | 29 | 19.3% |
| 10-50 GB/s | 45 | 30.0% |
| 50-150 GB/s | 49 | 32.7% |
| > 150 GB/s | 27 | 18.0% |

**Key Insight**: Client has fewer slow transitions (19.3% vs 32.0% for servers) and more sustained mid-tier performance (32.7% vs 25%), indicating better stability.

---

## 7. Latency Analysis

### Transition Duration

| Metric | Server 1 | Server 2 | Client |
|--------|----------|----------|--------|
| **Min** | 1,984 µs | 2,048 µs | 1,913 µs |
| **Avg** | 126,173 µs | 136,229 µs | 110,748 µs |
| **Max** | 1,009,846 µs | 1,132,117 µs | 1,131,606 µs |

**Observations**:
- Client has shortest **average** duration (110.7 ms vs 126-136 ms)
- Client is **13% faster** than server_1 and **19% faster** than server_2 in average duration
- Maximum durations are comparable across all entities (~1.1 seconds)
- Suggests client-side processing is more efficient

---

## 8. Key Findings and Insights

### Finding 1: Client Efficiency
**The client receiver is consistently more efficient than senders**
- 6-10% higher average throughput
- 13-19% faster average duration
- More stable performance distribution
- Fewer slow transitions

### Finding 2: Server Asymmetry Problem
**Significant performance differences between two servers**
- 8192B buffer: **server_1 is 2.52x faster** than server_2
- 2048B buffer: **server_2 is 1.52x faster** than server_1
- Suggests potential hardware differences or load imbalances
- Could benefit from load balancing or hardware tuning

### Finding 3: Optimal Buffer Size
**4096B buffer provides best performance across all entities**
- Server 1: 102.3 GB/s (highest for server_1)
- Server 2: 94.9 GB/s (excellent performance)
- Client: 124.1 GB/s (peak performance)
- Most balanced across entities

### Finding 4: Initialization Overhead
**Consistent ~870 MB/s minimum across all entities**
- Suggests shared initialization pattern
- Represents ~1/250th of peak performance
- Could be optimized for better startup performance

### Finding 5: Data Transfer Consistency
**All entities successfully transfer same total data volumes**
- Server 1: 104.43 GB
- Server 2: 104.43 GB
- Client: 208.86 GB (expected 2x for dual-server)
- Perfect data consistency verification ✓

---

## 9. Recommendations

### Immediate Actions
1. **Investigate Server 2 at 8192B Buffer**
   - Performance drops to 25.5 GB/s (5.9x variance)
   - Compare hardware specifications with Server 1
   - Check system load during test

2. **Optimize 4096B Buffer Configuration**
   - Use as standard for production
   - Provides best performance across all entities
   - Recommend making this the default buffer size

### Performance Optimization
3. **Reduce Initialization Overhead**
   - Current minimum: 870 MB/s
   - Consider pre-warming connections
   - Profile initialization phase to identify bottlenecks

4. **Load Balancing**
   - Consider distributing load more evenly between servers
   - Server 2 significantly underperforms at 8192B
   - May indicate resource contention or hardware limitation

5. **Client-Side Optimization**
   - Client outperforms servers - learn why
   - May apply insights to server-side code
   - Consider async batching strategies from client

### Monitoring
6. **Performance Baselines**
   - Server 1: 36.8 GB/s average
   - Server 2: 39.5 GB/s average
   - Client: 46.4 GB/s average
   - Alert if individual measurements drop below 10 GB/s

7. **Server Asymmetry Threshold**
   - Current max ratio: 2.52x (8192B)
   - Target: Keep asymmetry < 1.5x
   - Investigate any ratios > 2.0x

---

## 10. Technical Specifications

### Test Configuration
- **Servers**: 2 independent servers with identical buffer size tests
- **Client**: Single client simultaneously connected to both servers
- **Transition Points Measured**:
  - Server: WaitingForSendComplete → ReadyToTransfer
  - Client: WaitingForData → ReadyToReceive

### Data Format
```
source,bufSize,index,duration_us,bytes,throughput_MB_s
server_1,512,0,669567,536777088,757.46
server_1,512,1,27778,536691456,18425.70
server_2,512,0,580123,536777088,890.23
client,512,0,574782,1073468224,1781.09
```

### Calculation Methods
- **Throughput (MB/s)** = (bytes / duration_us) × (1,000,000 / 1,048,576)
- **Variance** = max_throughput / min_throughput
- **Ratio** = metric_B / metric_A

---

## Appendix: Performance Outliers

### Slowest Transitions (< 1 GB/s)
Found at initialization (index 0) across all entities and buffer sizes

### Fastest Transitions (> 200 GB/s)
- Server 1: 216.6 GB/s (4096B, index varies)
- Server 2: 219.3 GB/s (4096B, index varies)
- Client: 221.2 GB/s (4096B, index varies)

---

**Report Date**: November 7, 2024
**Data Source**: `/gpfs/zc2/u/dnb/velox/logs/2_servers/`
**CSV File**: `/gpfs/zc2/u/dnb/velox/performance_report_2_servers/2_servers_combined_transitions.csv`
**Total Records**: 300 transitions (75 server_1 + 75 server_2 + 150 client)
