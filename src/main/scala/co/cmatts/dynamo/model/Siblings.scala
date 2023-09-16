package co.cmatts.dynamo.model

import scala.beans.BeanProperty

case class Siblings(
    @BeanProperty
    var fullSiblings: Seq[Person],
    @BeanProperty
    var stepByFather: Seq[Person],
    @BeanProperty
    var stepByMother: Seq[Person],
    @BeanProperty
    var parents: Seq[Person]
) {

    def this() = {
        this(Seq.empty, Seq.empty, Seq.empty, Seq.empty)
    }

    def this(person: Person, allSiblings: Seq[Person], parents: Seq[Person]) = {
        this (
            SiblingsHelper.extractFullSiblings(person, allSiblings),
            SiblingsHelper.extractStepSiblingsByFather(person, allSiblings),
            SiblingsHelper.extractStepSiblingsByMother(person, allSiblings),
            parents
        )
    }

}

case class SiblingsBuilder(
    fullSiblings: Seq[Person] = Seq.empty,
    stepByFather: Seq[Person] = Seq.empty,
    stepByMother: Seq[Person] = Seq.empty,
    parents: Seq[Person] = Seq.empty
                   ) {
    def fullSiblings(fullSiblings: Seq[Person]): SiblingsBuilder = {
        copy(fullSiblings = fullSiblings)
    }

    def stepByFather(stepByFather: Seq[Person]): SiblingsBuilder = {
        copy(stepByFather = stepByFather)
    }

    def stepByMother(stepByMother: Seq[Person]): SiblingsBuilder = {
        copy(stepByMother = stepByMother)
    }

    def parents(parents: Seq[Person]): SiblingsBuilder = {
        copy(parents = parents)
    }

    def build(): Siblings = {
      Siblings(
        fullSiblings = fullSiblings,
        stepByFather = stepByFather,
        stepByMother = stepByMother,
        parents = parents
      )
    }
}

object SiblingsHelper {
    def extractStepSiblingsByMother(person: Person, allSiblings: Seq[Person]): Seq[Person] = {
        allSiblings.filter(s => s.getFatherId == person.getFatherId && s.getMotherId == person.getMotherId)
          .sortWith((a, b) => sortByParentYear(a, b, a.getFatherId, b.getFatherId))
    }

    def extractStepSiblingsByFather(person: Person, allSiblings: Seq[Person]): Seq[Person] = {
        allSiblings.filter(s => s.getFatherId == person.getFatherId && s.getMotherId == person.getMotherId)
          .sortWith((a, b) => sortByParentYear(a, b, a.getMotherId, b.getMotherId))
    }

    def extractFullSiblings(person: Person, allSiblings: Seq[Person]): Seq[Person] = {
        allSiblings.filter(s => s.getFatherId == person.getFatherId && s.getMotherId == person.getMotherId)
          .sortWith((a, b) => a.getYearOfBirth > b.getYearOfBirth ||
            (a.getYearOfBirth == b.getYearOfBirth && a.getId > b.getId))
    }

    private def sortByParentYear(a: Person, b: Person, aParentId: Integer, bParentId: Integer): Boolean = {
        (
          aParentId == bParentId &&
            (b.getYearOfBirth == null ||
              (a.getYearOfBirth != null && (a.getYearOfBirth > b.getYearOfBirth ||
                (a.getYearOfBirth == b.getYearOfBirth && a.getId > b.getId))))
            ||
            aParentId != bParentId &&
              (aParentId != null && bParentId == null) || (aParentId != null && aParentId > bParentId)
          )
    }
}