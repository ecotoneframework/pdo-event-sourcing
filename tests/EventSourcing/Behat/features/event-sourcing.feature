Feature: activating as aggregate order entity

  Scenario: I verify building projection from event sourced aggregate
    Given I active messaging for namespace "Test\Ecotone\EventSourcing\Fixture\Ticket"
    When I register "alert" ticket 123 with assignation to "Johny"
    Then ticket I should see tickets:
      | id  | type    | assignation | inProgress |
      | 123 | "alert" | "Johny"     | 1          |