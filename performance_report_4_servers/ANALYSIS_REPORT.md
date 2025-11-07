# 4-Server Performance Analysis Report

## Executive Summary

Analysis of a 4-server configuration where a single client simultaneously communicates with four independent servers. Each server measures the **WaitingForSendComplete → ReadyToTransfer** transition, while the client measures **WaitingForData → ReadyToReceive** transitions.

**Critical Finding**: Server 4 experienced a crash at 4096B buffer size (std::bad_weak_ptr exception), affecting data availability at this buffer size. The client achieves 2-3x higher throughput than individual servers at larger buffers (2048B-4096B), but only 0.92-1.12x at smaller buffers, indicating significant scaling issues.

---

## 1. Data Collection Overview

### Configuration
- **Setup**: Single client connecting to 4 independent servers simultaneously
- **Measurement Points**:
  - **Server Side**: WaitingForSendComplete → ReadyToTransfer (sender readiness)
  - **Client Side**: WaitingForData → ReadyToReceive (receiver readiness)
- **Buffer Sizes Tested**: 512B, 1024B, 2048B, 4096B (no 8192B for this scenario)

### Data Volume
| Entity | Transitions | Total Data | Status |
|--------|------------|----------|--------|
| Server 1 | 72 | 80.83 GB | ✓ Complete |
| Server 2 | 72 | 80.83 GB | ✓ Complete |
| Server 3 | 72 | 80.83 GB | ✓ Complete |
| Server 4 | 67 | 60.62 GB | ⚠️ Incomplete (missing 4096B) |
| Client | 72 | 80.83 GB | ✓ Complete |
| **Total** | **355** | **383.94 GB** | **⚠️ Server 4 Failed** |

**Critical Issue**: Server 4 crashed at 4096B buffer, resulting in 5 missing transitions.

---

## 2. Overall Performance Metrics

### Throughput Summary

| Metric | Server 1 | Server 2 | Server 3 | Server 4 | Client |
|--------|----------|----------|----------|----------|--------|
| **Min** | 442.5 MB/s | 441.8 MB/s | 440.4 MB/s | 442.8 MB/s | 449.8 MB/s |
| **Avg** | 29,787.4 MB/s | 31,075.2 MB/s | 31,219.2 MB/s | 31,844.2 MB/s | 36,609.1 MB/s |
| **Max** | 179,918.0 MB/s | 178,881.0 MB/s | 177,633.0 MB/s | 192,096.0 MB/s | 211,435.0 MB/s |
| **Variance** | 406.6x | 404.9x | 403.4x | 433.8x | 470.0x |

**Key Observations**:
- Client average throughput: **17.2% higher** than average of all servers
- Peak performance: Client 211.4 GB/s (highest across all entities)
- High variance (403-470x) indicates significant performance fluctuation
- Minimum throughput consistently ~440-450 MB/s (initialization overhead)

### Performance by Buffer Size

| Buffer | Avg Server | Client | Client Advantage |
|--------|-----------|--------|------------------|
| 512B | 40,216.4 MB/s | 41,244.0 MB/s | +1.3% (marginal) |
| 1024B | 19,491.5 MB/s | 24,060.2 MB/s | +23.4% (moderate) |
| 2048B | 21,191.0 MB/s | 43,403.9 MB/s | +104.9% (2x) ⭐ |
| 4096B | 21,435.8 MB/s | 35,480.3 MB/s | +65.3% (significant) |

**Key Findings**:
- **2048B buffer shows extreme client advantage: 2.02x faster**
- Client scales better with larger buffers
- Server 4 missing at 4096B prevents complete comparison

---

## 3. Server Performance Analysis

### Individual Server Throughput Metrics

**Server 1**:
- Average: 29,787.4 MB/s (slowest overall)
- Peak: 179,918.0 MB/s
- Best buffer: 512B (36.98 GB/s)
- Worst buffer: 4096B (16.24 GB/s)
- Variance: 406.6x

**Server 2**:
- Average: 31,075.2 MB/s
- Peak: 178,881.0 MB/s
- Best buffer: 512B (40.31 GB/s)
- Worst buffer: 1024B (16.64 GB/s) ⚠️ Performance cliff
- Variance: 404.9x

**Server 3**:
- Average: 31,219.2 MB/s
- Peak: 177,633.0 MB/s
- Best buffer: 512B (38.54 GB/s)
- Worst buffer: 4096B (14.05 GB/s) ⚠️ Worst performer
- Variance: 403.4x

**Server 4**:
- Average: 31,844.2 MB/s (before crash)
- Peak: 192,096.0 MB/s ⭐ Best peak performance
- Best buffer: 512B (45.03 GB/s) ⭐ Best at any buffer
- Status: **CRASHED at 4096B** ⚠️⚠️
- Variance: 433.8x

### Server-to-Server Asymmetry

**At 512B Buffer** (balanced):
- Best: Server 4 (45.03 GB/s)
- Worst: Server 1 (36.98 GB/s)
- Ratio: 1.22x (21% variance)

**At 1024B Buffer** (high variance):
- Best: Server 3 (26.19 GB/s)
- Worst: Server 4 (14.40 GB/s)
- Ratio: 1.82x (82% variance) ⚠️

**At 2048B Buffer** (degrading):
- Best: Server 1 (26.43 GB/s)
- Worst: Server 4 (14.89 GB/s)
- Ratio: 1.78x (78% variance) ⚠️

**At 4096B Buffer** (server_4 missing):
- Best: Server 2 (34.02 GB/s)
- Worst: Server 3 (14.05 GB/s)
- Ratio: 2.42x (142% variance) ⚠️⚠️

**Key Finding**: Server asymmetry increases dramatically at larger buffers, reaching 2.42x at 4096B.

---

## 4. Critical Issue: Server 4 Crash

### Status
- **Transitions Missing**: 5 (at 4096B buffer)
- **Completion Rate**: 67/72 (93.1%)
- **Error**: std::bad_weak_ptr exception
- **Implication**: Cannot complete 4-server comparison at 4096B

### Impact on Analysis
1. Cannot verify if Server 4 pattern continues at 4096B
2. Client vs servers comparison incomplete at 4096B
3. Server asymmetry analysis limited to 512B-2048B
4. Recommendation: Investigate and fix Server 4 crash

### Error Analysis
- **Timing**: Occurs specifically at 4096B buffer (largest)
- **Pattern**: Completes 512B, 1024B, 2048B successfully
- **Hypothesis**: Memory/resource exhaustion at larger buffers
- **Action Required**: Debug std::bad_weak_ptr exception

---

## 5. Client Performance Analysis

### Client Overall Performance
- **Average Throughput**: 36,609.1 MB/s
- **Peak Throughput**: 211,435.0 MB/s ⭐
- **Minimum Throughput**: 449.8 MB/s
- **Performance Variance**: 470.0x (highest)

### Client by Buffer Size

**512B Buffer**:
- Throughput: 41,244.0 MB/s
- Advantage vs servers: +1.3% (marginal - essentially equal)
- Status: Balanced performance

**1024B Buffer**:
- Throughput: 24,060.2 MB/s
- Advantage vs servers: +23.4% (moderate advantage)
- Status: Beginning of scaling advantage

**2048B Buffer**:
- Throughput: 43,403.9 MB/s ⭐ **Client scales better**
- Advantage vs servers: +105% (2x faster)
- Minimum server: Server 4 at 14.89 GB/s
- Maximum server: Server 1 at 26.43 GB/s
- Status: Significant scaling advantage

**4096B Buffer**:
- Throughput: 35,480.3 MB/s
- Advantage vs servers: +65.3% (substantial)
- Note: Cannot compare to Server 4 (crashed)
- Status: Strong advantage maintained

### Key Finding: Non-Linear Scaling
Client throughput does NOT scale linearly with buffer size:
- 512B: 41.2 GB/s
- 1024B: 24.1 GB/s (↓ 42%)
- 2048B: 43.4 GB/s (↑ 80%)
- 4096B: 35.5 GB/s (↓ 18%)

This pattern suggests tuning issues or cache interactions at 1024B.

---

## 6. Client vs Servers Detailed Comparison

### Performance Ratio (Client / Server) by Buffer

**512B Buffer**:
- vs Server 1: 1.12x
- vs Server 2: 1.02x (essentially equal)
- vs Server 3: 1.07x
- vs Server 4: 0.92x ⚠️ (Server 4 faster)
- **Conclusion**: At 512B, client not significantly better

**1024B Buffer**:
- vs Server 1: 1.16x
- vs Server 2: 1.45x
- vs Server 3: 0.92x ⚠️ (Server 3 faster)
- vs Server 4: 1.67x ⭐
- **Conclusion**: High variance; no clear pattern

**2048B Buffer**:
- vs Server 1: 1.64x
- vs Server 2: 1.98x ⭐
- vs Server 3: 2.02x ⭐⭐ (Nearly 2x faster)
- vs Server 4: 2.92x ⭐⭐⭐
- **Conclusion**: Client dramatically outperforms servers

**4096B Buffer**:
- vs Server 1: 2.19x ⭐
- vs Server 2: 1.04x (essentially equal)
- vs Server 3: 2.52x ⭐⭐ (2.5x faster)
- vs Server 4: NO DATA ⚠️
- **Conclusion**: Highly variable; Server 2 catches up

---

## 7. Performance Issues and Anomalies

### Issue 1: Server 4 Crash at 4096B
- **Severity**: CRITICAL
- **Impact**: Cannot complete 4-server analysis
- **Root Cause**: std::bad_weak_ptr exception
- **Action**: Debug and fix immediately

### Issue 2: Server 1 Performance Degradation
- **Pattern**: Worst performer across buffers
- **At 4096B**: 16.24 GB/s (lowest among active servers)
- **Variance**: 406.6x (indicating instability)
- **Possible Cause**: Resource exhaustion or bottleneck

### Issue 3: 1024B Buffer Anomaly
- **Observation**: Lowest average across all servers and client
- **Server 2 particularly bad**: 16.64 GB/s
- **Server 4 particularly bad**: 14.40 GB/s
- **Hypothesis**: Cache misalignment or memory access pattern issue

### Issue 4: Client Non-Linear Scaling
- **Pattern**: 512B > 2048B > 4096B > 1024B
- **Expected**: Monotonic increase with buffer size
- **Actual**: 1024B is performance valley
- **Implication**: Possible buffer management tuning issue

### Issue 5: High Performance Variance
- **All entities**: 403-470x variance
- **Indicates**: Significant unpredictability
- **Target**: Reduce to < 100x for production

---

## 8. Data Consistency Verification

### Volume Check
- Server 1: 80.83 GB ✓
- Server 2: 80.83 GB ✓
- Server 3: 80.83 GB ✓
- Server 4: 60.62 GB (incomplete due to crash) ⚠️
- Client: 80.83 GB ✓
- **Status**: Consistent except Server 4

### Record Count
- Server 1: 72 records ✓
- Server 2: 72 records ✓
- Server 3: 72 records ✓
- Server 4: 67 records (5 missing at 4096B) ⚠️
- Client: 72 records ✓
- **Status**: 355 total (5 missing from Server 4)

---

## 9. Key Findings Summary

### Finding 1: Client Outperforms at Large Buffers
- 512B: +1.3% (marginal)
- 1024B: +23.4% (moderate)
- 2048B: +105% (2x) ⭐
- 4096B: +65.3% (substantial)
- **Implication**: Use 2048B-4096B buffers for best client performance

### Finding 2: Significant Server Asymmetry
- 4096B: 2.42x variation (Server 2 vs Server 3)
- Limiting factor: Server 3 underperformance
- Pattern: Worse at larger buffers
- **Implication**: Investigate server hardware/configuration

### Finding 3: Server 4 is Critical Failure
- Crashes at 4096B buffer
- std::bad_weak_ptr exception
- 67/72 transitions (93.1% complete)
- **Implication**: Cannot deploy 4-server configuration for large buffers

### Finding 4: 1024B Buffer Has Issues
- Lowest throughput across all servers
- High variance between servers
- Possible cache/alignment issue
- **Implication**: Avoid 1024B; use 512B, 2048B, or 4096B

### Finding 5: Client Scaling is Better
- Client maintains advantage at larger buffers
- Server performance degrades faster
- Client reaches 211 GB/s peak (highest)
- **Implication**: Client-side optimizations apply to server-side

---

## 10. Recommendations

### Immediate (Critical)
1. **Fix Server 4 Crash**
   - Debug std::bad_weak_ptr exception
   - Test 4096B buffer thoroughly
   - Prevent production deployment until fixed

2. **Investigate Server 1 Performance**
   - Lowest overall average (29.8 GB/s)
   - Significant variance (406.6x)
   - Compare hardware/configuration with other servers

### High Priority
3. **Optimize 1024B Buffer Performance**
   - Root cause analysis for performance valley
   - Investigate cache alignment and memory access patterns
   - Consider skipping 1024B in production

4. **Reduce Performance Variance**
   - Current: 403-470x
   - Target: < 100x
   - Profile initialization and peak performance phases

### Medium Priority
5. **Scale to 4 Servers Safely**
   - Only after Server 4 crash is fixed
   - Implement monitoring for std::bad_weak_ptr errors
   - Test at scale before production

6. **Apply Client Optimizations**
   - Analyze client code for efficiency insights
   - Client outperforms servers by 2x at 2048B
   - Transfer patterns to server implementation

### Operational
7. **Set Performance SLA**
   - Minimum: 15 GB/s (production threshold)
   - Average target: 30+ GB/s
   - Alert on variance > 300x

8. **Monitoring and Alerting**
   - Monitor all 4 servers for exceptions
   - Track buffer-specific performance
   - Alert on Server 4 failures immediately

---

## 11. Technical Details

### Test Configuration
- **Servers**: 4 independent servers
- **Client**: Single client connecting to all 4
- **Buffers**: 512B, 1024B, 2048B, 4096B (no 8192B)
- **Failure**: Server 4 at 4096B (std::bad_weak_ptr)

### Data Format
```
source,bufSize,index,duration_us,bytes,throughput_MB_s
server_1,512,0,669567,536777088,757.46
server_2,512,0,580123,536777088,890.23
server_3,512,0,595432,536777088,854.33
server_4,512,0,574782,536777088,916.54
client,512,0,574782,1073468224,1781.09
```

### Performance Calculation
```
Throughput (MB/s) = (bytes / duration_us) × (1,000,000 / 1,048,576)
```

---

## Appendix: Performance by Server and Buffer

### Server 1 Details
- 512B: 36.98 GB/s (good)
- 1024B: 20.73 GB/s (degraded)
- 2048B: 26.43 GB/s (recovered)
- 4096B: 16.24 GB/s (worst)
- **Overall**: Consistent degradation with larger buffers

### Server 2 Details
- 512B: 40.31 GB/s (excellent)
- 1024B: 16.64 GB/s (worst performer)
- 2048B: 21.92 GB/s (recovered)
- 4096B: 34.02 GB/s (best at 4096B)
- **Overall**: High volatility; best at 4096B

### Server 3 Details
- 512B: 38.54 GB/s (good)
- 1024B: 26.19 GB/s (best at 1024B)
- 2048B: 21.53 GB/s (degraded)
- 4096B: 14.05 GB/s (worst overall)
- **Overall**: Consistent degradation pattern

### Server 4 Details (Incomplete)
- 512B: 45.03 GB/s (best at 512B) ⭐
- 1024B: 14.40 GB/s (worst at 1024B)
- 2048B: 14.89 GB/s (recovered slightly)
- 4096B: CRASHED ⚠️
- **Overall**: Variable, then failure

---

**Report Date**: November 7, 2024
**Data Source**: `/gpfs/zc2/u/dnb/velox/logs/4_servers/`
**CSV File**: `/gpfs/zc2/u/dnb/velox/performance_report_4_servers/4_servers_combined_transitions.csv`
**Total Records**: 355 transitions (5 missing from Server 4 at 4096B)
**Data Volume**: 383.94 GB (60.62 GB from incomplete Server 4)
