Feature: activating as aggregate order entity

  Scenario: I verify building projection from event sourced aggregate
    Given I active messaging for namespace "Test\Ecotone\EventSourcing\Fixture\Ticket"
    When I register "alert" ticket 123 with assignation to "Johny"
    Then ticket I should see in progress tickets:
      | ticket_id  | ticket_type    |
      | 123        | alert          |
    When I close ticket with id 123
    Then ticket I should see in progress tickets:
      | ticket_id  | ticket_type    |