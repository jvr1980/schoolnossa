# Pass 0 – Web Research Prompt

**Used in:** Lovable webapp (`research-school-descriptions` step) and `school_description_pipeline.py`
**Model:** Perplexity Sonar (web search enabled)
**Output:** Raw free-text description (input to Pass 1)

---

Research the school "{schulname}" in {city}, Germany using Google Search.
{The school's website is: {website} | The school does not have a known website.}

Known data about this school:
- Name: {schulname}
- Type: {school_type}
- Ownership: {traegerschaft} (public/private)
- District: {bezirk}
- Neighborhood: {ortsteil}
- Founded: {gruendungsjahr}
- Special features: {besonderheiten}
- Languages offered: {sprachen}
- Student count (2024/25): {schueler_2024_25}
- Teacher count (2024/25): {lehrer_2024_25}

INSTRUCTIONS:
1. Search the web for information about this school, especially from {their website | any official sources}.
2. Write a comprehensive, detailed, and up-to-date description of this school in ENGLISH following the template structure below.
3. Only include sections and details you can verify or reasonably infer. Omit sections where you have no information rather than guessing.
4. If the school has limited online presence, use the known data above to write as complete a description as possible.
5. Write in a professional, factual, parent-friendly tone.
6. The description should be rich and informative — aim for 400-800 words.

TEMPLATE (use this structure, adapt sections based on available information):

[School Name] is a [public/private/international/independent] school located in [City, Country], serving students from [age range or grade levels]. Founded in [year], the school has built a strong reputation for [core strengths, e.g. academic excellence, holistic education, international outlook], and currently educates approximately [number] students across [number] year groups.

The school's educational approach is guided by its mission to [mission statement in plain language], placing a strong emphasis on [values such as curiosity, responsibility, respect, creativity, or global citizenship].

Educational Philosophy and Values

At the heart of [School Name] is a commitment to [student-centered learning / academic rigor / holistic development]. The school believes that education should not only prepare students for academic success, but also equip them with the skills, mindset, and character needed to thrive in a rapidly changing world.

Key values promoted throughout school life include [e.g. integrity, inclusivity, perseverance, collaboration], which are embedded in both classroom learning and the wider school community.

Curriculum and Academic Program

[School Name] offers a [national / international / bilingual / blended] curriculum aligned with [national education standards / IB / Cambridge / local authority] requirements. Students follow a structured academic program covering core subjects such as [languages, mathematics, sciences, humanities], complemented by a broad range of elective and enrichment subjects.

Special academic features may include:

[Bilingual or multilingual instruction]
[Advanced or gifted programs]
[Individual learning support or differentiation]
[Project-based or inquiry-based learning]

Assessment and feedback are used to support continuous progress, with a strong focus on [personal development, critical thinking, and independent learning].

Language and International Orientation (if applicable)

The school places particular importance on [language learning / international-mindedness]. Instruction is delivered in [primary language(s) of instruction], with additional languages introduced from [grade level] onward.

With a community representing [number or diversity of nationalities], [School Name] fosters a multicultural environment that encourages openness, cultural awareness, and global perspectives.

Teaching Staff and Learning Environment

The teaching staff at [School Name] consists of [qualified, experienced, internationally trained] educators who are committed to continuous professional development. Teachers work closely with students to provide [individualized support, mentoring, and constructive feedback], ensuring that each learner can reach their full potential.

Class sizes are typically [small / moderate / capped at X students], allowing for a supportive and engaging learning environment.

Facilities and Resources

The school campus is equipped with [modern / well-maintained / purpose-built] facilities designed to support both academic and extracurricular activities. These may include:

[Science laboratories]
[Library and learning resource center]
[Sports halls, outdoor fields, or swimming pool]
[Art, music, and drama spaces]
[Technology labs or digital learning tools]

Digital learning is supported through [learning platforms, devices, or blended learning approaches], enabling students to develop strong digital literacy skills.

Extracurricular Activities and Student Life

Beyond the classroom, [School Name] offers a wide range of extracurricular activities that support students' interests and talents. These may include [sports teams, music ensembles, drama productions, clubs, and academic competitions].

Participation in extracurricular activities is encouraged as a way to foster [confidence, teamwork, leadership, and personal responsibility], contributing to a vibrant and well-rounded school experience.

Student Support and Wellbeing

Student wellbeing is a key priority at [School Name]. The school provides structured support systems such as [pastoral care, counseling services, learning support, or mentoring programs] to ensure that students feel safe, supported, and valued.

Close collaboration between teachers, students, and parents helps create a strong partnership focused on [academic success and emotional wellbeing].

Community and Parental Involvement

[School Name] values a close and collaborative relationship with parents and guardians. Regular communication, [parent-teacher conferences, workshops, events], and opportunities for involvement help foster a strong sense of community.

The school also maintains connections with [local organizations, cultural institutions, or international partners], enriching students' learning beyond the classroom.

Preparation for the Future

Graduates of [School Name] are well prepared for [upper secondary education, university, vocational pathways, or international opportunities]. The school supports students in making informed future choices through [career guidance, academic counseling, and transition programs].

Alumni typically go on to [universities, careers, or pathways] that reflect the school's commitment to [academic excellence and personal development].

Write the description now. Output ONLY the description text, no headers like "Description:" or markdown formatting.
