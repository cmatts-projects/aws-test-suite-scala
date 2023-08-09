package co.cmatts.dynamo

import co.cmatts.dynamo.model.{Fact, FactBuilder, Person, PersonBuilder, Siblings, SiblingsBuilder}


object DynamoDbTestDataFactory {
  private val PEOPLE_DATA = Seq(
    Seq("Mr Test", 16, 17, 1900, 1990),
    Seq("Mr Test1", 16, 17, 1890, 1990),
    Seq("Mr Test2", 16, null, 1900, 1990),
    Seq("Mr Test3", 16, 20, 1900, 1990),
    Seq("Mr Test4", 16, 21, 1900, 1990),
    Seq("Mr Test5", 16, 21, 1890, 1990),
    Seq("Mr Test6", 16, null, 1900, 1990),
    Seq("Mr Test10", null, 17, 1900, 1990),
    Seq("Mr Test11", 18, 17, 1900, 1990),
    Seq("Mr Test12", 19, 17, 1900, 1990),
    Seq("Mr Test13", 19, 17, 1890, 1990),
    Seq("Mr Test14", null, 17, 1900, 1990),
    Seq("Mr Test15", 14, 15, 1900, 1990),
    Seq("Mr Test16", null, null, 1880, null),
    Seq("Mr Test17", null, null, null, 1920),
    Seq("Mr Test20", null, null, null, null),
    Seq("Mr Test21", null, null, null, null),
    Seq("Mr Test22", null, null, null, null),
    Seq("Mr Test23", null, null, null, null),
    Seq("Mr Test24", null, null, null, null),
    Seq("Mr Test25", null, null, null, null),
    Seq("First Person", null, null, null, null)
  )
  private val FACT_DATA = Seq(
    Seq(1, 1901, "resource2", null, "fact1"),
    Seq(1, 1902, "resource1", null, "fact2"),
    Seq(1, 1892, null, "source1", "fact3"),
    Seq(2, 1852, "resource3", null, "fact4"),
    Seq(3, 1872, "resource4", null, "fact5"),
    Seq(21, 1872, "Original", null, "some facts")
  )
  val PERSON_1_SIBLINGS: Siblings = SiblingsBuilder()
    .fullSiblings(Seq(person(2), person(1)))
    .stepByFather(Seq(person(3), person(7), person(4), person(6), person(5)))
    .stepByMother(
      Seq(person(8), person(12), person(9), person(11), person(10))
    )
    .parents(
      Seq(
        person(16),
        person(17),
        person(18),
        person(19),
        person(20),
        person(21)
      )
    )
    .build()
  val PERSON_3_SIBLINGS: Siblings = SiblingsBuilder()
    .fullSiblings(Seq(person(3), person(7)))
    .stepByFather(Seq(person(2), person(1), person(4), person(6), person(5)))
    .parents(Seq(person(16), person(17), person(20), person(21)))
    .build()

  val PERSON_8_SIBLINGS: Siblings = SiblingsBuilder()
    .fullSiblings(Seq(person(8), person(12)))
    .stepByMother(
      Seq(person(2), person(1), person(9), person(11), person(10))
    )
    .parents(Seq(person(16), person(17), person(18), person(19)))
    .build()

  def peopleDataList: Seq[Person] = 0.until(peopleCount).map(i => person(i + 1))

  def person(i: Int): Person = {
    val index = i - 1
    PersonBuilder()
      .id(i)
      .name(PEOPLE_DATA(index)(0).asInstanceOf[String])
      .fatherId(PEOPLE_DATA(index)(1).asInstanceOf[Integer])
      .motherId(PEOPLE_DATA(index)(2).asInstanceOf[Integer])
      .yearOfBirth(PEOPLE_DATA(index)(3).asInstanceOf[Integer])
      .yearOfDeath(PEOPLE_DATA(index)(4).asInstanceOf[Integer])
      .version(1L)
      .build()
  }

  def factDataList: Seq[Fact] = 0.until(factCount).map(i => fact(i + 1))

  def fact(i: Int): Fact = {
    val index = i - 1
    FactBuilder()
      .id(i)
      .personId(FACT_DATA(index)(0).asInstanceOf[Integer])
      .year(FACT_DATA(index)(1).asInstanceOf[Integer])
      .image(FACT_DATA(index)(2).asInstanceOf[String])
      .source(FACT_DATA(index)(3).asInstanceOf[String])
      .description(FACT_DATA(index)(4).asInstanceOf[String])
      .version(1L)
      .build()
  }

  def peopleCount: Int = PEOPLE_DATA.length

  def factCount: Int = FACT_DATA.length
}
