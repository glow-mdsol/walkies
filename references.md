# References

Published literature underpinning the clinical assumptions and analyses in this
project, which correlates walking/hiking activity (Garmin FIT, GPX) against
glucose and insulin data (Carelink CSV) for a Type 1 diabetic on a MiniMed 780G
hybrid closed-loop system.

Citations follow **Vancouver (ICMJE)** style — the standard convention for
biomedical/clinical references. Article identifiers were verified against PubMed.
Each entry notes how it informs the project.

**Contents:** exercise & glucose guidance (1–3) · CGM metrics (4) · exercise
physiology (5) · AID & exercise (6) · walking (7) · algorithmic glucose control /
SmartGuard (8–10) · MiniMed 780G device (11–12) · open-source AID (13–16, incl.
inspectable OpenAPS algorithm source) · closed-loop during exercise (17–18) ·
exercise-type & endurance physiology (19–20) · correspondence (21–22).

> Journal references had their DOI/PMID verified against PubMed. Entries 15–16 are
> software/web sources (the open-source algorithm itself); access dates are noted
> in place of DOIs.
>
> 📄 **Local PDFs.** Entries marked **[PDF: `sources/…`]** have a full-text copy
> downloaded into the `sources/` folder (gitignored — not committed to the repo).

---

## Foundational guidance — exercise & glucose in type 1 diabetes

1. Riddell MC, Gallen IW, Smart CE, Taplin CE, Adolfsson P, Lumb AN, et al.
   Exercise management in type 1 diabetes: a consensus statement.
   *Lancet Diabetes Endocrinol*. 2017;5(5):377–390.
   doi:[10.1016/S2213-8587(17)30014-1](https://doi.org/10.1016/S2213-8587(17)30014-1).
   PMID: 28126459. **[PDF: `sources/Exercise management in T1d.pdf`]**
   *The definitive consensus on exercise in T1D — pre-exercise glucose targets,
   carbohydrate/insulin adjustment, and aerobic-vs-anaerobic glucose responses.
   Underpins the clinical thresholds in `CLAUDE.md` and the hypo-risk analyses.
   See the reply/letter correspondence at refs 20–21.*

2. Colberg SR, Sigal RJ, Yardley JE, Riddell MC, Dunstan DW, Dempsey PC, et al.
   Physical activity/exercise and diabetes: a position statement of the American
   Diabetes Association.
   *Diabetes Care*. 2016;39(11):2065–2079.
   doi:[10.2337/dc16-1728](https://doi.org/10.2337/dc16-1728). PMID: 27926890.
   *ADA position statement on activity and diabetes — broad evidence base framing
   the activity-vs-glycaemia analyses.*

3. Moser O, Riddell MC, Eckstein ML, Adolfsson P, Rabasa-Lhoret R, van den Boom L,
   et al. Glucose management for exercise using continuous glucose monitoring
   (CGM) and intermittently scanned CGM (isCGM) systems in type 1 diabetes:
   position statement of the European Association for the Study of Diabetes (EASD)
   and of the International Society for Pediatric and Adolescent Diabetes (ISPAD),
   endorsed by JDRF and supported by the American Diabetes Association (ADA).
   *Diabetologia*. 2020;63(12):2501–2520.
   doi:[10.1007/s00125-020-05263-9](https://doi.org/10.1007/s00125-020-05263-9).
   PMID: 33047169.
   *CGM-specific exercise guidance: trend-arrow thresholds, sensor lag, and the
   glucose ranges to act on. Directly relevant to interpreting the CGM trace
   during a walk.*

## CGM metrics & glycaemic targets

4. Battelino T, Danne T, Bergenstal RM, Amiel SA, Beck R, Biester T, et al.
   Clinical targets for continuous glucose monitoring data interpretation:
   recommendations from the International Consensus on Time in Range.
   *Diabetes Care*. 2019;42(8):1593–1603.
   doi:[10.2337/dci19-0028](https://doi.org/10.2337/dci19-0028). PMID: 31177185.
   *Standardises time-in-range (TIR), time-below-range, and time-above-range.
   Defines the summary statistics the analysis computes (TIR, hypo/hyper time
   during vs outside activity).*

## Exercise physiology & glucose dynamics

5. Tonoli C, Heyman E, Roelands B, Buyse L, Cheung SS, Berthoin S, et al.
   Effects of different types of acute and chronic (training) exercise on
   glycaemic control in type 1 diabetes mellitus: a meta-analysis.
   *Sports Med*. 2012;42(12):1059–1080.
   doi:[10.1007/BF03262312](https://doi.org/10.1007/BF03262312). PMID: 23134339.
   *Meta-analytic effect sizes for how exercise type/intensity moves glucose —
   population benchmarks to compare individual walks against.*

## Automated insulin delivery (closed-loop) & exercise

6. Zaharieva DP, Messer LH, Paldus B, O'Neal DN, Maahs DM, Riddell MC.
   Glucose control during physical activity and exercise using closed loop
   technology in adults and adolescents with type 1 diabetes.
   *Can J Diabetes*. 2020;44(8):740–749.
   doi:[10.1016/j.jcjd.2020.06.003](https://doi.org/10.1016/j.jcjd.2020.06.003).
   PMID: 33011134. **[PDF: `sources/Zaharieva-et-al.-2020.pdf`]**
   *Strategies for exercising safely on automated insulin delivery (AID) systems —
   the most directly applicable reference for the MiniMed 780G in AUTO mode.
   Explains how AID handles pre-exercise insulin reduction and the limits of
   automation around activity, which is what the app's basal/SmartGuard overlay
   visualises.*

## Walking specifically

7. DiPietro L, Gribok A, Stevens MS, Hamm LF, Rumpler W. Three 15-min bouts of
   moderate postmeal walking significantly improves 24-h glycemic control in
   older people at risk for impaired glucose tolerance.
   *Diabetes Care*. 2013;36(10):3262–3268.
   doi:[10.2337/dc13-0084](https://doi.org/10.2337/dc13-0084). PMID: 23761134.
   *Demonstrates that even short walking bouts meaningfully lower glucose and
   blunt postprandial excursions — the core mechanism this app visualises.
   (Cohort is older adults at risk of glucose intolerance, not T1D, so treat as
   mechanistic/contextual rather than directly transferable.)*

## Algorithmic glucose control & predictive insulin suspension (SmartGuard-style)

These describe how the MiniMed family of algorithms (low-glucose suspend → predictive
low-glucose management → advanced hybrid closed-loop) actually decides to reduce or
suspend insulin — i.e. the mechanism behind the `smartguard` suspend/predicted-low
events this app parses from the Carelink CSV.

8. Bergenstal RM, Klonoff DC, Garg SK, Bode BW, Meredith M, Slover RH, et al.
   Threshold-based insulin-pump interruption for reduction of hypoglycemia.
   *N Engl J Med*. 2013;369(3):224–232.
   doi:[10.1056/NEJMoa1303576](https://doi.org/10.1056/NEJMoa1303576).
   PMID: 23789889.
   *The ASPIRE In-Home RCT — foundational evidence for the threshold-suspend
   ("Low Glucose Suspend") feature that is the direct ancestor of SmartGuard.
   Establishes that automatically stopping insulin at a sensor threshold reduces
   hypoglycaemia without raising HbA1c — the rationale for the suspend events the
   app flags during walks.*

9. Choudhary P, de Portu S, Arrieta A, Castañeda J, Campbell FM. Use of
   sensor-integrated pump therapy to reduce hypoglycaemia in people with Type 1
   diabetes: a real-world study in the UK.
   *Diabet Med*. 2019;36(9):1100–1108.
   doi:[10.1111/dme.14043](https://doi.org/10.1111/dme.14043). PMID: 31134668.
   *Real-world Carelink-uploaded data from MiniMed 640G SmartGuard users,
   quantifying how each automation tier (suspend-on-low vs predictive low-glucose
   management) cuts time below range. Closely mirrors this project's data source
   and the predicted-low-suspend vs suspend distinction the parser makes.*

10. Telliam C, Thivolet C. Pumps that predict and manage low blood glucose are
    superior to pumps with stand-alone CGM for reducing hypoglycaemia in type 1
    diabetes patients in a real-world setting.
    *Diabetes Metab*. 2021;47(3):101168.
    doi:[10.1016/j.diabet.2020.05.010](https://doi.org/10.1016/j.diabet.2020.05.010).
    PMID: 32497708.
    *Real-world comparison of MiniMed 640G SmartGuard predictive low-glucose
    suspend against stand-alone flash monitoring — supports interpreting suspend
    events as protective rather than as data noise.*

## MiniMed 780G — the specific device in this project

These document what is publicly stated about the advanced hybrid closed-loop (AHCL)
algorithm in the MMT-1885 pump used here. Note: Medtronic has **not** published the
exact SmartGuard control equations (they are proprietary); these papers evidence the
algorithm's *design intent and clinical outcomes*, not the literal code.

11. Collyns OJ, Meier RA, Betts ZL, Chan DSH, Frampton C, Frewen CM, et al.
    Improved glycemic outcomes with Medtronic MiniMed advanced hybrid closed-loop
    delivery: results from a randomized crossover trial comparing automated insulin
    delivery with predictive low glucose suspend in people with type 1 diabetes.
    *Diabetes Care*. 2021;44(4):969–975.
    doi:[10.2337/dc20-2250](https://doi.org/10.2337/dc20-2250). PMID: 33579715.
    *The pivotal head-to-head of the 780G's AHCL mode (auto basal + auto correction
    boluses) against its own predictive low-glucose suspend (PLGS) mode. Documents
    exactly the algorithm states this app's `smartguard` parser distinguishes
    (predicted-low-suspend vs suspend vs AUTO microbolus), and the TIR/time-below
    figures it should expect.*

12. Carlson AL, Sherr JL, Shulman DI, Garg SK, Pop-Busui R, Bode BW, et al.
    Safety and glycemic outcomes during the MiniMed advanced hybrid closed-loop
    system pivotal trial in adolescents and adults with type 1 diabetes.
    *Diabetes Technol Ther*. 2022;24(3):178–189.
    doi:[10.1089/dia.2021.0319](https://doi.org/10.1089/dia.2021.0319).
    PMID: 34694909.
    *The 780G regulatory pivotal trial — establishes the device's safety profile
    and baseline glycaemic performance in AUTO mode, the reference against which an
    individual's walk-day glucose control can be contextualised.*

## Open-source / DIY automated insulin delivery

The "#WeAreNotWaiting" community built open algorithms (OpenAPS, Loop, AndroidAPS)
that perform the same predictive dosing/suspension role as SmartGuard. These are the
relevant precedents for any open re-implementation of closed-loop control logic.

13. Braune K, Lal RA, Petruželková L, Scheiner G, Winterdijk P, Schmidt S, et al.
    Open-source automated insulin delivery: international consensus statement and
    practical guidance for health-care professionals.
    *Lancet Diabetes Endocrinol*. 2022;10(1):58–74.
    doi:[10.1016/S2213-8587(21)00267-9](https://doi.org/10.1016/S2213-8587(21)00267-9).
    PMID: 34785000.
    *The reference overview of the open-source AID landscape (OpenAPS, Loop,
    AndroidAPS) — how the community-built algorithms work, their evidence base, and
    safety/regulatory considerations. The starting point for understanding open
    re-implementations of SmartGuard-style control.*

14. Burnside MJ, Lewis DM, Crocket HR, Meier RA, Williman JA, Sanders OJ, et al.
    Open-source automated insulin delivery in type 1 diabetes.
    *N Engl J Med*. 2022;387(10):869–881.
    doi:[10.1056/NEJMoa2203913](https://doi.org/10.1056/NEJMoa2203913).
    PMID: 36069869.
    *The CREATE trial — the first RCT of an open-source AID system (OpenAPS 0.7.0
    algorithm in AndroidAPS). Demonstrates that a community-built, open algorithm
    safely raises time-in-range vs sensor-augmented pump therapy: evidence that
    open SmartGuard-equivalent control is clinically viable.*

### Open algorithm source (grey literature / software)

Unlike SmartGuard, the open-source algorithms are fully inspectable. These are the
most useful primary sources if you want to verify or reproduce the control logic
itself (e.g. the prediction + temp-basal/suspend decision behind a hypo event).

15. OpenAPS. *oref0: the open reference implementation of the OpenAPS reference
    design* [software]. The Open Artificial Pancreas System; 2015– [cited 2026 May
    30]. Available from: <https://github.com/openaps/oref0>.
    Core dosing logic: `lib/determine-basal/determine-basal.js`. Reference-design
    rationale: <https://github.com/openaps/docs/blob/master/reference-design.md>.
    *The readable, open analogue to SmartGuard: given recent CGM, insulin-on-board,
    carbs and profile settings, `determine-basal` forecasts glucose and issues
    temp-basal / suspend recommendations — directly comparable to the predicted-low
    and suspend events this app surfaces.*

16. OpenAPS. *OpenAPS documentation — Understanding the determine-basal logic*
    [Internet]. [cited 2026 May 30]. Available from:
    <https://openaps.readthedocs.io/en/latest/docs/While%20You%20Wait%20For%20Gear/Understand-determine-basal.html>.
    *Plain-language walkthrough of the prediction and dosing-decision algorithm —
    the clearest published description of how an open closed-loop controller reasons
    about impending lows, useful for sanity-checking this app's assumptions.*

## Closed-loop / AID performance during exercise

Direct evidence on how automated insulin delivery behaves under sustained, intense
activity — the scenario closest to Geoffrey's longer hill walks.

17. Breton MD, Cherñavvsky DR, Forlenza GP, DeBoer MD, Robic J, Wadwa RP, et al.
    Closed-loop control during intense prolonged outdoor exercise in adolescents
    with type 1 diabetes: the Artificial Pancreas Ski Study.
    *Diabetes Care*. 2017;40(12):1644–1650.
    doi:[10.2337/dc17-0883](https://doi.org/10.2337/dc17-0883). PMID: 28855239.
    **[PDF: `sources/closed loop during porlonged intense exercise.pdf`]**
    *RCT of closed-loop control during extended vigorous outdoor exercise (skiing)
    — the closest analogue to multi-hour hill walking, showing both the protective
    effect and the limits of automation when activity is prolonged and intense.*

18. van den Boom L, Ziko H, Mader JK. Safely finishing a half marathon by an adult
    with type 1 diabetes using a commercially available hybrid closed-loop system.
    *J Diabetes Investig*. 2021;12(3):450–453.
    doi:[10.1111/jdi.13356](https://doi.org/10.1111/jdi.13356). PMID: 32652903.
    **[PDF: `sources/J of Diabetes Invest - 2020 - Boom - Safely finishing a half marathon by an adult with type 1 diabetes using a commercially.pdf`]**
    *Case report of completing endurance exercise on a commercial hybrid closed-loop
    pump using added carbohydrate plus a temporary raised glucose target — a concrete
    worked example of the strategy this app is meant to help review after the fact.*

## Exercise-type & endurance physiology

19. Parent C, Lespagnol E, Berthoin S, Tagougui S, Stuckens C, Tonoli C, et al.
    Continuous moderate and intermittent high-intensity exercise in youth with
    type 1 diabetes: which protection for dysglycemia?
    *Diabetes Res Clin Pract*. 2024;210:111631.
    doi:[10.1016/j.diabres.2024.111631](https://doi.org/10.1016/j.diabres.2024.111631).
    PMID: 38513989.
    **[PDF: `sources/Continuous-moderate-and-intermittent-high-intensity-ex_2024_Diabetes-Researc.pdf`]**
    *Compares continuous-moderate vs intermittent-high-intensity exercise on glucose
    — supports interpreting the HR-zone analysis (steady walking vs bursty climbs)
    against differing dysglycaemia risk. (Open access.)*

20. Scott SN, Fontana FY, Cocks M, Morton JP, Jeukendrup A, Dragulin R, et al.
    Post-exercise recovery for the endurance athlete with type 1 diabetes: a
    consensus statement.
    *Lancet Diabetes Endocrinol*. 2021;9(5):304–317.
    doi:[10.1016/S2213-8587(21)00054-1](https://doi.org/10.1016/S2213-8587(21)00054-1).
    PMID: 33864810.
    **[PDF: `sources/Post-exercise-recovery-for-the-endurance-athlete-wi_2021_The-Lancet-Diabetes.pdf`]**
    *Consensus on the post-exercise window — late/overnight hypo risk, refuelling and
    insulin adjustment after endurance activity. Justifies the app's post-walk
    analysis window extending beyond the activity itself.*

## Correspondence (re: Riddell 2017 consensus, ref 1)

21. Campbell MD, Kime N, McKenna J. Exercise and physical activity in patients
    with type 1 diabetes. *Lancet Diabetes Endocrinol*. 2017;5(7):493.
    doi:[10.1016/S2213-8587(17)30169-9](https://doi.org/10.1016/S2213-8587(17)30169-9).
    PMID: 28533137.
    **[PDF: `sources/Exercise + physcial activity in T1D.pdf`]**
    *Letter questioning how applicable the consensus statement (ref 1) is to the
    "average" person with T1D rather than athletes — useful caveat when applying
    consensus thresholds to everyday walks.*

22. Riddell MC, Gallen IW, Rabasa-Lhoret R. Exercise and physical activity in
    patients with type 1 diabetes — authors' reply.
    *Lancet Diabetes Endocrinol*. 2017;5(7):493–494.
    doi:[10.1016/S2213-8587(17)30168-7](https://doi.org/10.1016/S2213-8587(17)30168-7).
    PMID: 28533138.
    **[PDF: `sources/Exercise-and-physical-activity-in-patients-with-t_2017_The-Lancet-Diabetes--.pdf`]**
    *The consensus authors' response to ref 21.*

---

*Citation identifiers verified via PubMed (e.g. <https://pubmed.ncbi.nlm.nih.gov/28126459/>).
To add a reference, confirm it on PubMed and record the DOI + PMID in the same
Vancouver format above.*
