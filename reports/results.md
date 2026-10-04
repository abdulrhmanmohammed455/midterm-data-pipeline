# تقرير مخرجات خط البيانات الهجين (Hybrid Big Data Pipeline Report)

## 1. ملخص المشروع
تم بناء خط بيانات هجين لمعالجة بيانات الطلبات الضخمة وفق معمارية ELT، مع دعم التوجيه الديناميكي بين Python Batch و Apache Spark، وتطبيق قواعد الجودة، وضمان الـ Idempotency عبر عمليات الـ Upsert في MongoDB.

---

## 2. مقارنة الأداء بين المحركين (Engine Comparison)

| وجه المقارنة | Python Batch Loader | Apache Spark Loader |
| :--- | :--- | :--- |
| **الملف المستخدم** | `data/orders_sample.csv`[cite: 1] | `data/orders_huge_mixed_quality.csv`[cite: 1] |
| **حجم الملف** | ~40 MB[cite: 1] | 12.65 GB[cite: 1] |
| **إجمالي السجلات** | 100,000 سجل[cite: 1] | 30,000,000 سجل[cite: 1] |
| **المحرك المختار** | `python_batch`[cite: 1] | `pyspark`[cite: 1] |
| **سبب الاختيار** | الحجم <= 200 MB[cite: 1] | الحجم > 200 MB[cite: 1] |
| **معدل المعالجة (Throughput)** | ~1,205 سجل / ثانية | ~1,519 سجل / ثانية |
| **إدارة الذاكرة** | Streaming Generator (DictReader)[cite: 1] | تقطيع متوازٍ وتوزيع الذاكرة عبر DataFrames[cite: 1] |

---

## 3. مقاييس الجودة والتصنيف (Data Quality & Classification)

* **السجلات الخام المحملة (Raw Loaded):** 30,100,000 سجل (تم حفظها بنسبة 100% دون حذف مسبق)[cite: 1].
* **السجلات السليمة (Valid):** 1,381,788 سجل (نُقلت مباشرة إلى `orders_validated`)[cite: 1].
* **السجلات المصححة (Corrected):** 1,764,082 سجل (عولجت آلياً مع حفظ أثر الـ Audit Trail)[cite: 1].
* **السجلات المعزولة (Quarantine):** 264,130 سجل (تم عزلها في `orders_quarantine` مع تحديد أسباب العزل)[cite: 1].
* **معادلة الاتساق (Consistency Metric):** تحقق كامل بنتيجة **`PASS`**[cite: 1]:
  $$\text{Raw Count} = \text{Valid Count} + \text{Corrected Count} + \text{Quarantine Count}$$[cite: 1]

---

## 4. إثبات الـ Idempotency و Upsert
تم إثبات خاصية الـ Idempotency عبر إعادة تشغيل نفس الملف مرتين متتاليتين[cite: 1]:
* **التشغيل الأول:** `Inserted: 91,716` و `Unchanged: 0`[cite: 1].
* **إعادة التشغيل (Rerun):** `Inserted: 0` و `Unchanged: 90,799`[cite: 1].
* **النتيجة:** عدم توليد أي سجلات مكررة (No duplicate business records) وثبات الحالة التجارية النهائية في `orders_validated`[cite: 1].